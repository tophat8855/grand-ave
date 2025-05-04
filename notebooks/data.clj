;; # Data preparation

(ns data
  (:require [tablecloth.api :as tc]
            [camel-snake-kebab.core :as csk]
            [geo.io :as geoio]
            [charred.api :as charred]
            [geo.spatial :as spatial]
            [clojure.string :as str])
  (:import (org.locationtech.jts.geom Geometry)))

;; ## Crashes

^{:kindly/hide-code true}
(def crash-csv-files
  ["datasets/2015crashes.csv"
   "datasets/2016crashes.csv"
   "datasets/2017crashes.csv"
   "datasets/2018crashes.csv"
   "datasets/2019crashes.csv"
   "datasets/2020crashes.csv"
   "datasets/2021crashes.csv"
   "datasets/2022crashes.csv"
   "datasets/2023crashes.csv"
   "datasets/2024crashes.csv"
   "datasets/2025crashes.csv"])

^{:kindly/hide-code true}
(def parties-csv-files
  ["datasets/2015parties.csv"
   "datasets/2016parties.csv"
   "datasets/2017parties.csv"
   "datasets/2018parties.csv"
   "datasets/2019parties.csv"
   "datasets/2020parties.csv"
   "datasets/2021parties.csv"
   "datasets/2022parties.csv"
   "datasets/2023parties.csv"
   "datasets/2024parties.csv"
   "datasets/2025parties.csv"])

^{:kindly/hide-code true}
(def injured-witness-passengers-csv-files
  ["datasets/2015injuredwitnesspassengers.csv"
   "datasets/2016injuredwitnesspassengers.csv"
   "datasets/2017injuredwitnesspassengers.csv"
   "datasets/2018injuredwitnesspassengers.csv"
   "datasets/2019injuredwitnesspassengers.csv"
   "datasets/2020injuredwitnesspassengers.csv"
   "datasets/2021injuredwitnesspassengers.csv"
   "datasets/2022injuredwitnesspassengers.csv"
   "datasets/2023injuredwitnesspassengers.csv"
   "datasets/2024injuredwitnesspassengers.csv"
   "datasets/2025injuredwitnesspassengers.csv"])

(def telegraph-intersections-of-interest
  #{"19TH" "20TH" "21ST" "22ND" "23RD" "24TH" "25TH" "26TH"
    "27TH" "28TH" "29TH" "30TH" "31ST" "32ND" "33RD" "34TH"
    "35TH" "36TH" "37TH" "38TH" "39TH" "40TH" "41ST"})


^{:kindly/hide-code true}
(defn load-and-combine-csvs [file-paths]
  (let [datasets (map #(tc/dataset % {:key-fn    csk/->kebab-case-keyword
                                      :parser-fn {:collision-id       :integer
                                                  :crash-date-time    :local-date-time
                                                  :ncic-code          :integer
                                                  :is-highway-related :boolean
                                                  :is-tow-away        :boolean
                                                  :number-injured     :integer
                                                  :number-killed      :integer}})
                      file-paths)]
    (apply tc/concat datasets)))


(def oakland-city-crashes
  (-> (load-and-combine-csvs crash-csv-files)
      (tc/select-columns [:collision-id
                          :ncic-code
                          :crash-date-time
                          :collision-type-description
                          :day-of-week
                          :is-highway-related
                          :motor-vehicle-involved-with-desc
                          :motor-vehicle-involved-with-other-desc
                          :number-injured
                          :number-killed
                          :lighting-description
                          :latitude
                          :longitude
                          :pedestrian-action-desc
                          :primary-road
                          :secondary-road])))


(delay
  (-> oakland-city-crashes
      :pedestrian-action-desc
      frequencies))

;; ## Oakland neighborhoods:

(def neighborhoods
  (-> "data/Features_20250425.csv.gz"
      (tc/dataset {:key-fn keyword})
      (tc/map-columns :geometry
                      [:the_geom]
                      (fn [the-geom]
                        (-> the-geom
                            str
                            geoio/read-wkt)))
      (tc/select-columns [:geometry :Name])))

;; 

(defn make-spatial-index [dataset & {:keys [geometry-column]
                                     :or   {geometry-column :geometry}}]
  (let [tree (org.locationtech.jts.index.strtree.STRtree.)]
    (doseq [row (tc/rows dataset :as-maps)]
      (let [geometry (row geometry-column)]
        (.insert tree
                 (.getEnvelopeInternal geometry)
                 (assoc row
                        :prepared-geometry
                        (org.locationtech.jts.geom.prep.PreparedGeometryFactory/prepare geometry)))))
    tree))

(def neighborhoods-index
  (make-spatial-index neighborhoods))

(defn intersecting-places [region spatial-index]
  (->> (.query spatial-index (.getEnvelopeInternal region))
       (filter (fn [row]
                 (.intersects (:prepared-geometry row) region)))
       tc/dataset))

;; ## Oakland street centerlines

;; Using defonce to run this data filtering only once per session:
(defonce filter-geojson
  (-> "data/Street_Centerlines_-8203296818607454791.geojson"
      slurp
      (charred/read-json {:key-fn keyword})
      (update :features (partial filter (fn [{:keys [geometry properties]}]
                                          (and geometry
                                               (let [{:keys [CITYR CITYL]}
                                                     properties]
                                                 (and (or (= CITYL "Oakland")
                                                          (= CITYR "Oakland"))))))))
      (->> (charred/write-json "data/Oakland-centerlines.geojson"))))

;; ## Coordinate transformations
;; (between globe coordinates and local approximate plane coordinates):

(def crs-transform-wgs84->bay-area
  (geo.crs/create-transform
   ;; https://epsg.io/4326
   (geo.crs/create-crs 4326)
   ;; https://epsg.io/2227
   (geo.crs/create-crs 2227)))

(def crs-transform-bay-area->wgs84
  (geo.crs/create-transform
   ;; https://epsg.io/2227
   (geo.crs/create-crs 2227)
   ;; https://epsg.io/4326
   (geo.crs/create-crs 4326)))

(defn wgs84->bay-area
  [geometry]
  (geo.jts/transform-geom geometry crs-transform-wgs84->bay-area))

(defn bay-area->wgs84
  [geometry]
  (geo.jts/transform-geom geometry crs-transform-bay-area->wgs84))


;; ## Preprocessing Oakland street centerlines

(def Oakland-centerlines
  (let [geojson-str (slurp "data/Oakland-centerlines.geojson")]
    (-> geojson-str
        geoio/read-geojson
        (->> (map (fn [{:keys [properties geometry]}]
                    (assoc properties :geometry geometry))))
        tc/dataset
        (tc/map-columns :line-string
                        [:geometry]
                        #(spatial/to-jts % 4326))
        (tc/map-columns :local-line-string
                        [:line-string]
                        wgs84->bay-area)
        (tc/map-columns :local-buffer
                        [:local-line-string]
                        (fn [^Geometry g]
                          (.buffer g 50)))
        (tc/add-column :line-string-geojson
                       (-> geojson-str
                           (charred/read-json {:key-fn keyword})
                           :features
                           (->> (map :geometry ))))
        ;; Figure out relevant streets from the STREET field.
        ;; E.g.: if STREET="75TH ON HEGENBERGER EB",
        ;; then streets=["75TH" "HEGENBERGER"].
        (tc/map-columns :streets
                        [:STREET]
                        (fn [STREET]
                          (some-> STREET
                                  (str/replace #" (WB|NB|EB|SB)" " ")
                                  (str/replace #" CONN" " ")
                                  (str/split #" (ON|OFF|TO|FROM) ")
                                  (->> (mapv str/trim)))))
        (tc/map-columns :neighborhoods
                        [:line-string]
                        (fn [line-string]
                          (-> line-string
                              (data/intersecting-places data/neighborhoods-index)
                              :Name
                              vec))))))


(delay
  (-> Oakland-centerlines
      (tc/select-columns [:STREET :neighborhoods])))

(delay
  (->> Oakland-centerlines
       :neighborhoods
       (map count)
       frequencies))


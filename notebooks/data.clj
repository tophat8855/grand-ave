(ns data
  (:require [tablecloth.api :as tc]
            [camel-snake-kebab.core :as csk]
            [geo.io :as geoio]
            [charred.api :as charred]
            [geo.spatial :as spatial]
            [clojure.string :as str])
  (:import (org.locationtech.jts.geom Geometry)))

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

(def Oakland-centerlines
  (let [geojson-str (slurp "data/Oakland-centerlines.geojson")]
    (-> geojson-str
        geoio/read-geojson
        (->> (map (fn [{:keys [properties geometry]}]
                    (assoc properties :geometry geometry))))
        tc/dataset)))

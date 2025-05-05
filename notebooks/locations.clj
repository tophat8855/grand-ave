(ns locations
  (:require [aerial.hanami.templates :as ht]
            [camel-snake-kebab.core :as csk]
            [scicloj.kindly.v4.kind :as kind]
            [scicloj.tableplot.v1.plotly :as plotly]
            [scicloj.tableplot.v1.transpile :as transpile]
            [scicloj.tableplot.v1.hanami :as hanami]
            [tablecloth.api :as tc]
            [tablecloth.column.api :as tcc]
            [tech.v3.dataset :as ds]
            [scicloj.tablemath.v1.api :as tm]
            [clojure.math :as math]
            [fastmath.stats :as stats]
            [data]
            [charred.api :as charred]
            [clojure.string :as str]
            [geo
             [geohash :as geohash]
             [jts :as jts]
             [spatial :as spatial]
             [io :as geoio]
             [crs :as crs]]
            [std.lang :as l]
            [tech.v3.datatype :as dtype]
            [tech.v3.datatype.functional :as fun]
            [tech.v3.tensor :as tensor]
            [tech.v3.datatype.argops :as argops]
            [tech.v3.dataset.print :as print])
  (:import java.time.LocalDateTime
           (org.locationtech.jts.index.strtree STRtree)
           (org.locationtech.jts.geom Geometry Point Polygon Coordinate)
           (org.locationtech.jts.geom.prep PreparedGeometry
                                           PreparedLineString
                                           PreparedPolygon
                                           PreparedGeometryFactory)
           (org.locationtech.jts.algorithm Centroid)))

;; How many centerlines?

(tc/row-count data/Oakland-centerlines)

;; What classes?

(-> data/Oakland-centerlines
    (tc/group-by [:CLASS])
    (tc/aggregate {:n tc/row-count})
    (tc/order-by [:n] :desc))

;; In the data preprocessing, we mapped
;; the `STREET` field into a `street` field,
;; enumerating all relevant streets.

;; For example:

(-> data/Oakland-centerlines
    (tc/select-rows #(-> % :streets count (= 1)))
    (tc/select-columns [:STREET :streets])
    (tc/random 10 {:seed 1}))

(-> data/Oakland-centerlines
    (tc/select-rows #(-> % :streets count (> 1)))
    (tc/select-columns [:STREET :streets])
    (tc/random 10 {:seed 1}))

;; We map each street to all relevant centerlines.

(def street->centerlines
  (-> data/Oakland-centerlines
      (tc/rows :as-maps)
      (->> (mapcat (fn [{:keys [streets]
                         :as centerline}]
                     (->> streets
                          (map (fn [street]
                                 [street centerline])))))
           (group-by first))
      (update-vals (partial map second))))

(count street->centerlines)

;; Let us focus on crashes data with pedestrians involved
;; and in the years where we have full data.

(def crashes
  (-> data/oakland-city-crashes
      (tc/select-rows (fn [{:keys [pedestrian-action-desc]}]
                        (some-> pedestrian-action-desc
                                (not= "NO PEDESTRIANS INVOLVED"))))
      (tc/map-columns :year
                      [:crash-date-time]
                      #(.getYear %))
      (tc/select-rows #(<= 2016 
                           (:year %)
                           2024))
      (tc/select-columns [:collision-id
                          :year
                          :longitude :latitude
                          :primary-road :secondary-road])))

(tc/shape crashes)

;; How often do they have coordinates information?

(-> crashes
    (tc/map-columns :has-coordinates
                    [:longitude :latitude]
                    (fn [lon lat]
                      (some? (and lon lat))))
    :has-coordinates
    frequencies)

;; Some geometry functions:

(defn centroid-point-distance [x centroid]
  (fun/distance x centroid))

(defn nearest [x centroids]
  (-> centroids
      (tensor/reduce-axis #(centroid-point-distance x %1) 1)
      argops/argmin))

;; Attach to the crashes data the relevant centerlines and intersections.

(def crashes-with-centerlines
  (-> crashes
      (tc/map-columns :centerlines
                      [:primary-road :secondary-road]
                      (fn [primary-road secondary-road]
                        (->> [primary-road secondary-road]
                             (map (fn [road]
                                    (some-> road
                                            str/upper-case
                                            (str/replace #"(-|W/B|N/B|E/B|S/B|WESTBOUND)" "")
                                            str/trim
                                            street->centerlines))))))
      (tc/map-columns :intersecting-segments
                      [:centerlines]
                      (fn [[centerlines1 centerlines2]]
                        (for [cl1 centerlines1
                              cl2 centerlines2
                              :when (.intersects ^Geometry (:local-buffer cl1)
                                                 ^Geometry (:local-buffer cl2))]
                          [cl1 cl2])))
      (tc/map-columns :intersection-centers
                      [:intersecting-segments]
                      (partial
                       map
                       (fn [cl1cl2]
                         (-> (let [[t1 t2]
                                   (map (fn [{:keys [local-line-string]}]
                                          (-> (->> local-line-string
                                                   jts/coordinates
                                                   (map (fn [^Coordinate c]
                                                          [(.getX c)
                                                           (.getY c)])))
                                              tensor/->tensor))
                                        cl1cl2)]
                               (-> (->> t1
                                        (map (fn [row1]
                                               (let [row2 (t2 (nearest row1 t2))]
                                                 [(fun/distance row1 row2) row1 row2])))
                                        (apply min-key first)
                                        rest)
                                   tensor/->tensor
                                   (tensor/reduce-axis fun/mean 0)
                                   (->> (apply jts/coordinate))
                                   jts/point
                                   data/bay-area->wgs84))))))
      (tc/map-columns :intersection-center
                      [:intersecting-segments]
                      (fn [intersecting-segments]
                        (some-> intersecting-segments
                                (->> (mapcat (fn [cl1cl2]
                                               (-> (mapcat (fn [{:keys [local-line-string]}]
                                                             (->> local-line-string
                                                                  jts/coordinates
                                                                  (map (fn [^Coordinate c]
                                                                         [(.getX c)
                                                                          (.getY c)]))))
                                                           cl1cl2)))))
                                seq
                                tensor/->tensor
                                (tensor/reduce-axis fun/mean 0)
                                (->> (apply jts/coordinate))
                                jts/point
                                data/bay-area->wgs84)))))

(-> crashes-with-centerlines
    :intersecting-segments
    (->> (map count)))



;; How often can we find intersection data?

(-> crashes-with-centerlines
    :intersecting-segments
    (->> (map (comp some? seq)))
    frequencies)

;; For example:

(def example-crash
  (-> crashes-with-centerlines
      (tc/rows :as-maps)
      first))

;; What roads intersect in this crash?

(select-keys example-crash
             [:primary-road :secondary-road])

;; How many street segments do we have for each of the
;; two roads in tersecting at this crash?

(-> example-crash
    :centerlines
    (->> (map count)))

;; How many pairs of itnersecting segments do we have out of those?

(-> example-crash
    :intersecting-segments
    count)

;; What are the approximate points of those intersections?

(-> example-crash
    :intersection-centers)

;; What one single center point should we use to draw this intersection over a map?

(-> example-crash
    :intersection-center)

;; Some auxiliary JS transpilation:

(defn js
  [& forms]
  ((l/ptr :js)
   (cons 'do forms)))

(defn- js-assignment [symbol data]
  (format "let %s = %s;"
          symbol
          (charred/write-json-str data)))

(defn- js-entry-assignment [symbol0 symbol1 symbol2]
  (format "let %s = %s['%s'];"
          symbol0
          symbol1
          symbol2))

(defn- js-closure [js-statements]
  (->> js-statements
       (str/join "\n")
       (format "(function () {\n%s\n})();")))

;; Let us draw a few example intersections:

(delay
  (-> crashes-with-centerlines
      (tc/random 10 {:seed 1})
      (tc/select-rows #(:intersection-center %))
      (tc/rows :as-maps)
      (->> (map (fn [{:keys [primary-road
                             secondary-road
                             intersecting-segments
                             intersection-centers
                             intersection-center]}]
                  (let [segments-geojson {:features (-> intersecting-segments
                                                        (->> (mapcat
                                                              (fn [l1l2]
                                                                (->> l1l2
                                                                     (mapv :line-string-geojson))))
                                                             vec))}
                        center-coordinate (jts/coord intersection-center)
                        centers-geojson {:features (->> intersection-centers
                                                        (mapv (fn [c]
                                                                {:type "Point"
                                                                 :coordinates [(.getX c)
                                                                               (.getY c)]})))}
                        data {'center [(.getY center-coordinate)
                                       (.getX center-coordinate)]
                              'zoom 16
                              'provider "OpenStreetMap.Mapnik"
                              'segments_geojson segments-geojson
                              'centers_geojson centers-geojson}]
                    [:div
                     [:h2 (str primary-road " - " secondary-road)]
                     (kind/hiccup
                      [:div {:style {:height "400px"}}
                       [:script
                        (js-closure
                         (concat
                          [(js-assignment 'data data)]
                          (->> data
                               (mapv (fn [[k v]]
                                       (js-entry-assignment k 'data k))))
                          [(js '(var m (L.map document.currentScript.parentElement))
                               '(m.setView center zoom)
                               '(-> (L.tileLayer.provider provider)
                                    (. (addTo m)))
                               '(-> segments_geojson
                                    (L.geoJSON)
                                    (. (addTo m)))
                               '(-> centers_geojson
                                    (L.geoJSON)
                                    (. (addTo m))))]))]])]))))
      (kind/hiccup {:html/deps [:leaflet]})))


;; Let us count the appearance of street segments in crashes:

(def year-segment-counts
  (-> crashes-with-centerlines
      (tc/rows :as-maps)
      (->> (mapcat (fn [{:keys [collision-id
                                year
                                intersecting-segments]}]
                     (->> intersecting-segments
                          (apply concat)
                          (map (fn [segment]
                                 {:collision-id collision-id
                                  :year year
                                  :segment segment}))))))
      tc/dataset
      (tc/group-by [:year :segment])
      (tc/aggregate {:n (fn [ds]
                          (-> ds
                              :collision-id
                              distinct
                              count))})))


(def segment-counts
  (-> year-segment-counts
      (tc/group-by [:segment])
      (tc/aggregate {:n (fn [ds]
                          (-> ds :n tcc/sum))
                     :year-counts (fn [ds]
                                    (-> ds
                                        (tc/select-columns [:year :n])
                                        (tc/order-by [:year])
                                        delay))})
      (tc/map-columns :year-counts [:year-counts] deref)
      (tc/map-columns :STREET [:segment] :STREET)))

;; What street segments are most frequent in this sense?

(-> segment-counts
    :n
    frequencies
    (->> (sort-by val)))

;; What are the year-by-year pattern of the very frequent segments?

(-> segment-counts
    (tc/select-rows #(-> % :n (>= 20)))
    (tc/select-columns [:STREET :n :year-counts])
    (tc/order-by :STREET)
    (print/print-range :all))

;; Let us report the counts of the street segments of Grand Ave:

(def grand-ave-center
  [37.8080789,-122.2577377])

(delay
  (-> segment-counts
      (tc/select-rows #(-> % :STREET (= "GRAND AV")))
      (tc/order-by [:n] :desc)
      (tc/rows :as-maps)
      (->> (map (fn [{:keys [segment year-counts]}]
                  (let [{:keys [line-string-geojson]} segment
                        data {'center grand-ave-center
                              'zoom 16
                              'provider "OpenStreetMap.Mapnik"
                              'segment_geojson line-string-geojson}]
                    (kind/hiccup
                     [:div 
                      (-> year-counts
                          tc/dataset
                          (tc/order-by [:year])
                          kind/table)
                      [:div {:style {:height "400px"}}
                       [:script
                        (js-closure
                         (concat
                          [(js-assignment 'data data)]
                          (->> data
                               (mapv (fn [[k v]]
                                       (js-entry-assignment k 'data k))))
                          [(js '(var m (L.map document.currentScript.parentElement))
                               '(m.setView center zoom)
                               '(-> (L.tileLayer.provider provider)
                                    (. (addTo m)))
                               '(-> segment_geojson
                                    (L.geoJSON)
                                    (. (addTo m))))]))]]])))))
      (kind/fragment
       {:html/deps [:leaflet]})))


;; Combining with neighborhoods

(def year-streetneigh-counts
  (-> year-segment-counts
      (tc/map-columns :STREET [:segment] :STREET)
      (tc/map-columns :neighborhoods [:segment] :neighborhoods)
      (tc/rows :as-maps)
      (->> (mapcat (fn [{:as row
                         :keys [neighborhoods]}]
                     (->> neighborhoods
                          (map (fn [neigh]
                                 (-> row
                                     (assoc :neighborhood neigh)
                                     (dissoc :neighborhoods))))))))
      tc/dataset
      (tc/group-by [:year :STREET :neighborhood])
      (tc/aggregate {:n #(-> % :n tcc/sum)
                     :*segments #(-> % :segment vec delay)})
      (tc/group-by [:STREET :neighborhood])
      (tc/aggregate {:n #(-> % :n tcc/sum)
                     :year-counts (fn [ds]
                                    (-> ds
                                        (tc/select-columns [:year :n])
                                        (tc/order-by [:year])
                                        delay))
                     :*segments #(->> %
                                      :*segments
                                      (mapcat deref)
                                      delay)})
      (tc/map-columns :year-counts [:year-counts] deref)))


(delay
  (-> year-streetneigh-counts
      (tc/select-rows #(-> % :n (>= 20)))
      (tc/select-columns [:STREET :neighborhood :n :year-counts])
      (tc/order-by :STREET)
      (print/print-range :all)))

(delay
  (-> year-streetneigh-counts
      (tc/select-rows #(-> % :n (>= 50)))
      (tc/order-by [:n] :desc)
      (tc/rows :as-maps)
      first
      :*segments
      deref
      (->> (mapv (fn [{:keys [line-string-geojson]}]
                   {:type "Feature"
                    :geometry line-string-geojson})))))

(delay
  (-> year-streetneigh-counts
      (tc/select-rows #(-> % :n (>= 50)))
      (tc/order-by [:n] :desc)
      (tc/rows :as-maps)
      (->> (map (fn [{:keys [STREET neighborhood *segments n year-counts]}]
                  (let [geojson (->> @*segments
                                     (mapv (fn [{:keys [line-string-geojson]}]
                                             {:type "Feature"
                                              :geometry line-string-geojson})))
                        center (-> geojson
                                   (->> (mapcat (comp :coordinates :geometry)))
                                   tensor/->tensor
                                   (tensor/reduce-axis fun/mean 0)
                                   reverse)
                        data {'center center
                              'zoom 14
                              'provider "OpenStreetMap.Mapnik"
                              'segments_geojson geojson}]
                    [(kind/hiccup
                      [:div
                       [:h2 (str STREET " - " neighborhood)]
                       [:h3 "total: " (int n)]
                       [:h3 "segments: " (count @*segments)]
                       (-> year-counts
                           tc/dataset
                           (tc/order-by [:year])
                           (plotly/layer-line {:=x :year
                                               :=y :n}))])
                     (kind/hiccup
                      [:div {:style {:width "500px"
                                     :height "800px"}}
                       [:script
                        (js-closure
                         (concat
                          [(js-assignment 'data data)]
                          (->> data
                               (mapv (fn [[k v]]
                                       (js-entry-assignment k 'data k))))
                          [(js '(var m (L.map document.currentScript.parentElement))
                               '(m.setView center zoom)
                               '(-> (L.tileLayer.provider provider)
                                    (. (addTo m)))
                               '(-> segments_geojson
                                    (L.geoJSON {:style {:weight 10
                                                        :opacity 0.6}})
                                    (. (addTo m))))]))]])]))))
      (kind/table
       {:html/deps [:leaflet]})))










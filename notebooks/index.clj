^{:kindly/hide-code true
  :kindly/kind      :kind/hiccup}
(ns index
  (:require [aerial.hanami.templates :as ht]
            [camel-snake-kebab.core :as csk]
            [scicloj.kindly.v4.kind :as kind]
            [scicloj.tableplot.v1.plotly :as plotly]
            [scicloj.tableplot.v1.hanami :as hanami]
            [scicloj.tableplot.v1.transpile :as transpile]
            [tablecloth.api :as tc]
            [tablecloth.column.api :as tcc]
            [tech.v3.dataset :as ds]))

^{:kindly/hide-code true}
(import java.time.LocalDateTime)

^{:kindly/hide-code true
  :kindly/kind      :kind/hiccup}
(defn spy [x]
  (println x)
  x)

^{:kindly/hide-code true}
(defn load-and-combine-csvs [file-paths]
  (let [datasets (map #(ds/->dataset % {:key-fn    csk/->kebab-case-keyword
                                        :parser-fn {:collision-id       :integer
                                                    :crash-date-time    :local-date-time
                                                    :ncic-code          :integer
                                                    :is-highway-related :boolean
                                                    :is-tow-away        :boolean
                                                    :number-injured     :integer
                                                    :number-killed      :integer}})
                      file-paths)]
    (apply ds/concat datasets)))

^{:kindly/hide-code true}
(def crash-csv-files
  [#_"datasets/2015crashes.csv"
   "datasets/2016crashes.csv"
   "datasets/2017crashes.csv"
   "datasets/2018crashes.csv"
   "datasets/2019crashes.csv"
   "datasets/2020crashes.csv"
   "datasets/2021crashes.csv"
   "datasets/2022crashes.csv"
   "datasets/2023crashes.csv"
   "datasets/2024crashes.csv"
   #_"datasets/2025crashes.csv"])

^{:kindly/hide-code true}
(def parties-csv-files
  [#_"datasets/2015parties.csv"
   "datasets/2016parties.csv"
   "datasets/2017parties.csv"
   "datasets/2018parties.csv"
   "datasets/2019parties.csv"
   "datasets/2020parties.csv"
   "datasets/2021parties.csv"
   "datasets/2022parties.csv"
   "datasets/2023parties.csv"
   "datasets/2024parties.csv"
   #_"datasets/2025parties.csv"])

^{:kindly/hide-code true}
(def injured-witness-passengers-csv-files
  [#_"datasets/2015injuredwitnesspassengers.csv"
   "datasets/2016injuredwitnesspassengers.csv"
   "datasets/2017injuredwitnesspassengers.csv"
   "datasets/2018injuredwitnesspassengers.csv"
   "datasets/2019injuredwitnesspassengers.csv"
   "datasets/2020injuredwitnesspassengers.csv"
   "datasets/2021injuredwitnesspassengers.csv"
   "datasets/2022injuredwitnesspassengers.csv"
   "datasets/2023injuredwitnesspassengers.csv"
   "datasets/2024injuredwitnesspassengers.csv"
  #_ "datasets/2025injuredwitnesspassengers.csv"])


;; # Grand Ave, Oakland, CA

;; The City of Oakland is currently looking re-paving Grand Ave, and the local neighborhoods
;; and street safety groups are hoping that the re-paving will also include upgrades in the
;; pedestrian and bicycle infrastructure so that more neighbors are not lost on this street.

;; Let's look at Grand Ave, from Harrison to Mandana.

(def grand-intersections-of-interest
  {"HARRISON"    {:lat 37.810923 :lng -122.262360}
   "BAY PL"      {:lat 37.810590 :lng -122.260507}
   "PARK VIEW"   {:lat 37.809881 :lng -122.259373}
   "BELLEVUE"    {:lat 37.809713 :lng -122.259452}
   "LENOX"       {:lat 37.809358 :lng -122.258479}
   "LEE"         {:lat 37.809068 :lng -122.257263}
   "PERKINS"     {:lat 37.808994 :lng -122.256149}
   "ELLITA"      {:lat 37.808864 :lng -122.255016}
   "STATEN"      {:lat 37.808784 :lng -122.253832}
   "EUCLID"      {:lat 37.808608 :lng -122.251686}
   "EMBARCADERO" {:lat 37.809342 :lng -122.249697}
   "MACARTHUR"   {:lat 37.810195 :lng -122.248825}
   "LAKE PARK"   {:lat 37.811454 :lng -122.247977}
   "SANTA CLARA" {:lat 37.811797 :lng -122.247833}
   "ELWOOD"      {:lat 37.813721 :lng -122.246586}
   "MANDANA"     {:lat 37.814243 :lng -122.246230}})

(def grand-ave-crashes
  (let [crashes  (-> (load-and-combine-csvs crash-csv-files)
                     (ds/select-columns [:collision-id
                                         :ncic-code
                                         :crash-date-time
                                         :collision-type-description
                                         :day-of-week
                                         :is-highway-related
                                         :motor-vehicle-involved-with-code
                                         :motor-vehicle-involved-with-desc
                                         :motor-vehicle-involved-with-other-desc
                                         :number-injured
                                         :number-killed
                                         :lighting-description
                                         :latitude
                                         :longitude
                                         :pedestrian-action-desc
                                         :primary-road
                                         :secondary-road])
                     (ds/filter #(clojure.string/includes? (or (:primary-road %)
                                                               (:secondary-road %)) "GRAND"))
                     (ds/filter (fn [row]
                                  (some #(clojure.string/includes? (:secondary-road row) %)
                                        (keys grand-intersections-of-interest))))
                     spy)
        ]
    (-> crashes
        (tc/map-columns :intersection-lat
                        (tc/column-names crashes #{:secondary-road})
                        (fn [secondary-road]
                          (let [match (some (fn [[k v]]
                                              (when (clojure.string/includes? secondary-road k)
                                                v))
                                            grand-intersections-of-interest)]
                            (:lat match))))
        (tc/map-columns :intersection-lng
                        (tc/column-names crashes #{:secondary-road})
                        (fn [secondary-road]
                          (let [match (some (fn [[k v]]
                                              (when (clojure.string/includes? secondary-road k)
                                                v))
                                            grand-intersections-of-interest)]
                            (:lng match)))))))

;;; SLIDE GRAND AVE CRASH MAP
(def grand-ave-crash-to-heatmaps
  (mapv (fn [{:keys [intersection-lat intersection-lng row-count]}]
          [intersection-lat intersection-lng (* 0.9 row-count)])
        (-> grand-ave-crashes
            (tc/group-by [:intersection-lat :intersection-lng])
            (tc/aggregate {:row-count tc/row-count})
            (tc/rows :as-maps))))

(kind/hiccup
 [:div [:script
        {:src "https://cdn.jsdelivr.net/npm/leaflet.heat@0.2.0/dist/leaflet-heat.min.js"}]
  ['(fn [latlngs]
      [:div
       {:style {:height "500px"}
        :ref   (fn [el]
                 (let [m (-> js/L
                             (.map el)
                             (.setView (clj->js [37.811223 -122.253771])
                                       15.5))]
                   (-> js/L
                       .-tileLayer
                       (.provider "Stadia.AlidadeSmooth")
                       (.addTo m))
                   (doseq [latlng latlngs]
                     (-> js/L
                         (.marker (clj->js latlng))
                         (.addTo m)))
                   (-> js/L
                       (.heatLayer (clj->js latlngs)
                                   (clj->js {:radius 30}))
                       (.addTo m))))}])
   grand-ave-crash-to-heatmaps]]
;; Note we need to mention the dependency:
 {:html/deps [:leaflet]})

;;; SLIDE What are cars crashing into?

;; Types of crashes since 2016
(-> grand-ave-crashes
    (tc/group-by [:motor-vehicle-involved-with-desc])
    (tc/aggregate {:count tc/row-count})
    ((fn [df]
       (let [data (map (fn [row]
                         {:name (first row)
                          :value (second row)})
                  (tc/rows df))]
         (kind/echarts
          {:title {:text "Types of Crashes on Grand Ave 2016-2024"}
           :series {:type   "pie"
                    :data   data}})))))

;; Types of crashes over time
(-> grand-ave-crashes
    (ds/row-map (fn [row]
                  (let [date-time (:crash-date-time row)]
                    (assoc row
                           :year (str (.getYear date-time))))))
    (tc/group-by [:motor-vehicle-involved-with-desc :year ])
    (tc/aggregate {:count tc/row-count})
    ((fn [df]
       (let [years          (distinct (tc/column df :year))
             other-entities (filter some? (distinct (tc/column df :motor-vehicle-involved-with-desc)))
             data           (reduce (fn [acc typ]
                                      (assoc acc (keyword (csk/->kebab-case typ))
                                   (map (fn [year]
                                          (-> df
                                              (tc/select-rows #(and (= (:year %) year)
                                                                    (= (:motor-vehicle-involved-with-desc %) typ)))
                                              (tc/column :count)
                                              first
                                              (or 0)))
                                        years)))
                                    {:x-axis-data years}
                          other-entities)]
         (kind/echarts
          {:legend {:data (keys (dissoc data :x-axis-data))}
           :xAxis  {:type "category" :data years}
           :yAxis  {:type "value"}
           :series (map (fn [entity]
                          {:name entity
                           :type "bar"
                           :stack "total"
                           :data (get data (keyword (csk/->kebab-case entity)))})
                        (keys (dissoc data :x-axis-data)))})))))

(def ped-and-bike-codes
  "B is ped, G is bicycle"
  #{"B" "G"}) 

(def grand-ave-crashes-with-peds-and-cyclists
  (-> grand-ave-crashes
      (tc/select-rows (fn [row]
                        (ped-and-bike-codes (:motor-vehicle-involved-with-code row))))))

(-> grand-ave-crashes-with-peds-and-cyclists
    (ds/row-map (fn [row]
                  (let [date-time (:crash-date-time row)]
                    (assoc row
                           :year (str (.getYear date-time))))))
    (tc/group-by [:motor-vehicle-involved-with-desc :year ])
    (tc/aggregate {:count tc/row-count})
    ((fn [df]
       (let [years          (distinct (tc/column df :year))
             other-entities (filter some? (distinct (tc/column df :motor-vehicle-involved-with-desc)))
             data           (reduce (fn [acc typ]
                                      (assoc acc (keyword (csk/->kebab-case typ))
                                   (map (fn [year]
                                          (-> df
                                              (tc/select-rows #(and (= (:year %) year)
                                                                    (= (:motor-vehicle-involved-with-desc %) typ)))
                                              (tc/column :count)
                                              first
                                              (or 0)))
                                        years)))
                                    {:x-axis-data years}
                          other-entities)]
         (kind/echarts
          {:legend {:data (keys (dissoc data :x-axis-data))}
           :xAxis  {:type "category" :data years}
           :yAxis  {:type "value"}
           :series (map (fn [entity]
                          {:name entity
                           :type "bar"
                           :stack "total"
                           :data (get data (keyword (csk/->kebab-case entity)))})
                        (keys (dissoc data :x-axis-data)))})))))

(def grand-ave-injured
  (let [collision-ids        (-> grand-ave-crashes-with-peds-and-cyclists
                                 (tc/select-columns :collision-id)
                                 (tc/rows)
                                 flatten
                                 set)
        all-injured-on-grand (-> (load-and-combine-csvs injured-witness-passengers-csv-files)
                                 (ds/select-columns [:collision-id
                                                     :injured-wit-pass-id
                                                     :stated-age
                                                     :gender
                                                     :injured-person-type])
                                 (tc/select-rows (fn [row]
                                                   (contains? collision-ids (:collision-id row)))))

        possible-types        (-> all-injured-on-grand
                                  (tc/select-columns :injured-person-type)
                                  (tc/rows)
                                  flatten
                                  set)
        grand-with-crash-data (-> grand-ave-crashes-with-peds-and-cyclists
                                  (tc/select-columns [:collision-id
                                                      :crash-date-time
                                                      :motor-vehicle-involved-with-code
                                                      :motor-vehicle-involved-with-desc
                                                      :motor-vehicle-involved-with-other-desc
                                                      :number-injured
                                                      :number-killed
                                                      :lighting-description
                                                      :latitude
                                                      :longitude
                                                      :primary-road
                                                      :secondary-road]))
        crashes               (-> all-injured-on-grand
                                  (tc/select-rows (fn [row] (contains? #{"Pedestrian" "Bicyclist" "Other"} (:injured-person-type row))))
                                  (tc/inner-join grand-with-crash-data
                                                 {:left  :collision-id
                                                  :right :collision-id}))]
    (-> crashes
        (tc/map-columns :intersection-lat
                        (tc/column-names crashes #{:secondary-road})
                        (fn [secondary-road]
                          (let [match (some (fn [[k v]]
                                              (when (clojure.string/includes? secondary-road k)
                                                v))
                                            grand-intersections-of-interest)]
                            (:lat match))))
        (tc/map-columns :intersection-lng
                        (tc/column-names crashes #{:secondary-road})
                        (fn [secondary-road]
                          (let [match (some (fn [[k v]]
                                              (when (clojure.string/includes? secondary-road k)
                                                v))
                                            grand-intersections-of-interest)]
                            (:lng match)))))))

(def grand-ave-injuries-to-heatmaps
  (mapv (fn [{:keys [intersection-lat intersection-lng row-count]}]
          [intersection-lat intersection-lng (* 1.5 row-count)])
        (-> grand-ave-injured
            (tc/group-by [:intersection-lat :intersection-lng])
            (tc/aggregate {:row-count tc/row-count})
            (tc/rows :as-maps))))

;; SLIDE HEATMAP OF GRAND AVE INJURIES

(kind/hiccup
 [:div [:script
        {:src "https://cdn.jsdelivr.net/npm/leaflet.heat@0.2.0/dist/leaflet-heat.min.js"}]
  ['(fn [latlngs]
      [:div
       {:style {:height "500px"}
        :ref   (fn [el]
                 (let [m (-> js/L
                             (.map el)
                             (.setView (clj->js [37.811223 -122.253771])
                                       15.5))]
                   (-> js/L
                       .-tileLayer
                       (.provider "Stadia.AlidadeSmooth")
                       (.addTo m))
                   (doseq [latlng latlngs]
                     (-> js/L
                         (.marker (clj->js latlng))
                         (.addTo m)))
                   (-> js/L
                       (.heatLayer (clj->js latlngs)
                                   (clj->js {:radius 30}))
                       (.addTo m))))}])
   grand-ave-injuries-to-heatmaps]]
 ;; Note we need to mention the dependency:
 {:html/deps [:leaflet]})

;; This is ped and bike only
(-> grand-ave-injured
    (tc/update-columns :stated-age
                       #(tcc/replace-missing % :value -10))
    (plotly/layer-point
       {:=x      :crash-date-time
        :=y      :stated-age
        :=color  :injured-person-type
        :=layout {:title "Injuries on Grand Ave, by Age and Type"
                  :xaxis {:title "Date"}
                  :yaxis {:title "Age"}}}))

;; this is ped and bike only
(-> grand-ave-injured
    (tc/update-columns :stated-age
                       #(tcc/replace-missing % :value -10)) ;; has to be an int, so chose an in outside of age range
    (plotly/layer-histogram
     {:=x               :stated-age
      :=histnorm        "count"
      :=color           :injured-person-type
      :=mark-opacity    0.7
      :=layout          {:title "Injuries on Grand Ave, by Age and Type"
                         :xaxis {:title "Age"}
                         :yaxis {:title "Count"}}}))

;; # Telegraph Ave Crash Data

;; Using data from the [California Crash Reporting System (CCRS)](https://data.ca.gov/dataset/ccrs)

(def kono-intersections-of-interest
  {"19TH"      {:lat 37.808247 :lng -122.269923}
   "WILLIAM"   {:lat 37.808963 :lng -122.269773}
   "20TH"      {:lat 37.809594 :lng -122.269629}
   "BERKLEY"   {:lat 37.809594 :lng -122.269629}
   "21ST"      {:lat 37.810344 :lng -122.269407}
   "22ND"      {:lat 37.811187 :lng -122.269176}
   "GRAND"     {:lat 37.811892 :lng -122.269005}
   "23RD"      {:lat 37.812608 :lng -122.268827}
   "24TH"      {:lat 37.813726 :lng -122.268548}
   "25TH"      {:lat 37.814537 :lng -122.268354}
   "SYCAMORE"  {:lat 37.815051 :lng -122.268216}
   "26TH"      {:lat 37.815465 :lng -122.268126}
   "27TH"      {:lat 37.816191 :lng -122.267938}
   "MERRICMAC" {:lat 37.816621 :lng -122.267830}
   "28TH"      {:lat 37.817135 :lng -122.267710}
   "29TH"      {:lat 37.818240 :lng -122.267391}})

(def pill-hill-intersections-of-interest
  {"29TH"      {:lat 37.818240 :lng -122.267391}
   "30TH"      {:lat 37.819228 :lng -122.267149}
   "31ST"      {:lat 37.820018 :lng -122.266971}
   "32ND"      {:lat 37.820919 :lng -122.266696}
   "HAWTHORNE" {:lat 37.821349 :lng -122.266586}
   "33RD"      {:lat 37.821656 :lng -122.266498}
   "34TH"      {:lat 37.822472 :lng -122.266267}
   "35TH"      {:lat 37.823575 :lng -122.365950}
   "36TH"      {:lat 37.824620 :lng -122.265683}
   "37TH"      {:lat 37.825523 :lng -122.265444}
   "MACARTHUR" {:lat 37.826417 :lng -122.265178}
   "38TH"      {:lat 37.827045 :lng -122.265036}
   "APGAR"     {:lat 37.827550 :lng -122.264906}
   "39TH"      {:lat 37.828410 :lng -122.264681}
   "40TH"      {:lat 37.829167 :lng -122.264478}
   "41ST"      {:lat 37.29965 :lng -122.264254}})

(def telegraph-intersections-of-interest
  (merge kono-intersections-of-interest
         pill-hill-intersections-of-interest))

(def oakland-city-crashes
  (-> (load-and-combine-csvs crash-csv-files)
      (ds/select-columns [:collision-id
                          :ncic-code
                          :crash-date-time
                          :collision-type-description
                          :day-of-week
                          :is-highway-related
                          :motor-vehicle-involved-with-code
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

(def telegraph-ave-crashes
  (let [crashes (-> oakland-city-crashes
                     (ds/filter #(clojure.string/includes? (or (:primary-road %)
                                                               (:secondary-road %)) "TELEGRAPH"))
                     (ds/filter (fn [row]
                                  (or (some #(clojure.string/includes? (:primary-road row) %)
                                            (keys telegraph-intersections-of-interest))
                                      (some #(clojure.string/includes? (:secondary-road row) %)
                                            (keys telegraph-intersections-of-interest))))))]
    (-> crashes
    (tc/map-columns :intersection-lat
                        (tc/column-names crashes #{:secondary-road})
                        (fn [secondary-road]
                           (let [match (some (fn [[k v]]
                                               (when (clojure.string/includes? secondary-road k)
                                                 v))
                                            telegraph-intersections-of-interest)]
                             (:lat match))))
        (tc/map-columns :intersection-lng
                        (tc/column-names crashes #{:secondary-road})
                        (fn [secondary-road]
                           (let [match (some (fn [[k v]]
                                               (when (clojure.string/includes? secondary-road k)
                                                 v))
                                            telegraph-intersections-of-interest)]
                             (:lng match)))))))

(def telegraph-ave-crash-to-heatmaps
  (mapv (fn [{:keys [intersection-lat intersection-lng row-count]}]
          [intersection-lat intersection-lng (* 2 row-count)])
        (-> telegraph-ave-crashes
            (tc/group-by [:intersection-lat :intersection-lng])
            (tc/aggregate {:row-count tc/row-count})
            (tc/rows :as-maps))))

;; SLIDE TELEGRAPH AVE Map of crashes
;; ## Heatmap of Telegraph Ave crashes
(kind/hiccup
 [:div [:script
        {:src "https://cdn.jsdelivr.net/npm/leaflet.heat@0.2.0/dist/leaflet-heat.min.js"}]
  ['(fn [latlngs]
      [:div
       {:style {:height "500px"}
        :ref   (fn [el]
                 (let [m (-> js/L
                             (.map el)
                             (.setView (clj->js [37.821925 -122.266376])
                                       14.3))]
                   (-> js/L
                       .-tileLayer
                       (.provider "Stadia.AlidadeSmooth")
                       (.addTo m))
                   (doseq [latlng latlngs]
                     (-> js/L
                         (.marker (clj->js latlng))
                         (.addTo m)))
                   (-> js/L
                       (.heatLayer (clj->js latlngs)
                                   (clj->js {:radius 30}))
                       (.addTo m))))}])
   telegraph-ave-crash-to-heatmaps]]
 ;; Note we need to mention the dependency:
 {:html/deps [:leaflet]})

(def telegraph-ave-crashes-with-peds-and-cyclists
  (-> telegraph-ave-crashes
      (tc/select-rows (fn [row]
                        (ped-and-bike-codes (:motor-vehicle-involved-with-code row))))))

(def telegraph-ave-injured
  (let [collision-ids             (-> telegraph-ave-crashes-with-peds-and-cyclists
                                     (tc/select-columns :collision-id)
                                     (tc/rows)
                                     flatten
                                     set)
        all-injured-on-telegraph  (-> (load-and-combine-csvs injured-witness-passengers-csv-files)
                                     (ds/select-columns [:collision-id
                                                         :injured-wit-pass-id
                                                         :stated-age
                                                         :gender
                                                         :injured-person-type])
                                     (tc/select-rows (fn [row]
                                                       (contains? collision-ids (:collision-id row)))))
        possible-types            (-> all-injured-on-telegraph
                                     (tc/select-columns :injured-person-type)
                                     (tc/rows)
                                     flatten
                                     set)
        telegraph-with-crash-data (-> telegraph-ave-crashes-with-peds-and-cyclists
                                      (tc/select-columns [:collision-id
                                                          :crash-date-time
                                                          :motor-vehicle-involved-with-code
                                                          :motor-vehicle-involved-with-desc
                                                          :motor-vehicle-involved-with-other-desc
                                                          :number-injured
                                                          :number-killed
                                                          :lighting-description
                                                          :latitude
                                                          :longitude
                                                          :primary-road
                                                          :secondary-road]))
        crashes                   (-> all-injured-on-telegraph
                                     (tc/select-rows (fn [row] (contains? #{"Pedestrian" "Bicyclist" "Other"} (:injured-person-type row))))
                                     (tc/inner-join telegraph-with-crash-data
                                                    {:left  :collision-id
                                                     :right :collision-id}))]
    (-> crashes
        (tc/map-columns :intersection-lat
                        (tc/column-names crashes #{:secondary-road})
                        (fn [secondary-road]
                          (let [match (some (fn [[k v]]
                                              (when (clojure.string/includes? secondary-road k)
                                                v))
                                            telegraph-intersections-of-interest)]
                            (:lat match))))
        (tc/map-columns :intersection-lng
                        (tc/column-names crashes #{:secondary-road})
                        (fn [secondary-road]
                          (let [match (some (fn [[k v]]
                                              (when (clojure.string/includes? secondary-road k)
                                                v))
                                            telegraph-intersections-of-interest)]
                            (:lng match)))))))

(def telegraph-ave-injuries-to-heatmaps
  (mapv (fn [{:keys [intersection-lat intersection-lng row-count]}]
          [intersection-lat intersection-lng (* 2 row-count)])
        (-> telegraph-ave-injured
            (tc/group-by [:intersection-lat :intersection-lng])
            (tc/aggregate {:row-count tc/row-count})
            (tc/rows :as-maps))))

;; ## Heatmap of Telegraph Ave Injuries
(kind/hiccup
 [:div [:script
        {:src "https://cdn.jsdelivr.net/npm/leaflet.heat@0.2.0/dist/leaflet-heat.min.js"}]
  ['(fn [latlngs]
      [:div
       {:style {:height "500px"}
        :ref   (fn [el]
                 (let [m (-> js/L
                             (.map el)
                             (.setView (clj->js [37.821925 -122.266376])
                                       14.3))]
                   (-> js/L
                       .-tileLayer
                       (.provider "Stadia.AlidadeSmooth")
                       (.addTo m))
                   (doseq [latlng latlngs]
                     (-> js/L
                         (.marker (clj->js latlng))
                         (.addTo m)))
                   (-> js/L
                       (.heatLayer (clj->js latlngs)
                                   (clj->js {:radius 30}))
                       (.addTo m))))}])
   telegraph-ave-injuries-to-heatmaps]]
 ;; Note we need to mention the dependency:
 {:html/deps [:leaflet]})

;; ## Injuries in Oakland, over time (Including Telegraph), by month
(-> oakland-city-crashes
    (ds/row-map (fn [row]
                  (let [date-time (:crash-date-time row)]
                    (assoc row
                           :month-year (str (.getYear date-time) "-" (.getMonthValue date-time))))))
    (tc/dataset)
    (plotly/layer-bar
     {:=x :month-year
      :=y :number-injured}))

;; ## Injuries on Telegraph, over time, by month
(-> telegraph-ave-crashes
    (ds/row-map (fn [row]
                  (let [date-time (:crash-date-time row)]
                    (assoc row
                           :month-year (str (.getYear date-time) "-" (.getMonthValue date-time))))))
    (tc/dataset)
    (plotly/layer-bar
     {:=x :month-year
      :=y :number-injured}))


;; ## Injuries in Oakland, over time, by year
(-> oakland-city-crashes
    (ds/row-map (fn [row]
                  (let [date-time (:crash-date-time row)]
                    (assoc row
                           :year (str (.getYear date-time))))))
    (tc/dataset)
    (tc/group-by :year)
    (tc/aggregate {:number-injured-sum #(reduce + (map (fn [v] (if (nil? v) 0 (Integer. v))) (% :number-injured)))})
    (plotly/layer-bar
     {:=x :$group-name
      :=y :number-injured-sum
      :=layout {:title "Number of Injuries Over Years"
                :xaxis {:title "Year"}
                :yaxis {:title "Number of Injuries"}}}))

;; ## Injuries on Telegraph, over time, by year
(-> telegraph-ave-crashes
    (ds/row-map (fn [row]
                  (let [date-time (:crash-date-time row)]
                    (assoc row
                           :year (str (.getYear date-time))))))
    (tc/dataset)
    (tc/group-by :year)
    (tc/aggregate {:number-injured-sum #(reduce + (map (fn [v] (if (nil? v) 0 (Integer. v))) (% :number-injured)))})
    (plotly/layer-bar
     {:=x :$group-name
      :=y :number-injured-sum
      :=layout {:title "Number of Injuries Over Years"
                :xaxis {:title "Year"}
                :yaxis {:title "Number of Injuries"}}}))

;; ## Injuries in KONO, over time, by year
(def kono-crashes
  (let [crashes (-> oakland-city-crashes
                     (ds/filter #(clojure.string/includes? (or (:primary-road %)
                                                               (:secondary-road %)) "TELEGRAPH"))
                     (ds/filter (fn [row]
                                  (or (some #(clojure.string/includes? (:primary-road row) %)
                                            (keys kono-intersections-of-interest))
                                      (some #(clojure.string/includes? (:secondary-road row) %)
                                            (keys kono-intersections-of-interest))))))]
    (-> crashes
    (tc/map-columns :intersection-lat
                        (tc/column-names crashes #{:secondary-road})
                        (fn [secondary-road]
                           (let [match (some (fn [[k v]]
                                               (when (clojure.string/includes? secondary-road k)
                                                 v))
                                            kono-intersections-of-interest)]
                             (:lat match))))
        (tc/map-columns :intersection-lng
                        (tc/column-names crashes #{:secondary-road})
                        (fn [secondary-road]
                           (let [match (some (fn [[k v]]
                                               (when (clojure.string/includes? secondary-road k)
                                                 v))
                                            kono-intersections-of-interest)]
                             (:lng match)))))))

(-> kono-crashes
    (ds/row-map (fn [row]
                  (let [date-time (:crash-date-time row)]
                    (assoc row
                           :year (str (.getYear date-time))))))
    (tc/dataset)
    (tc/group-by :year)
    (tc/aggregate {:number-injured-sum #(reduce + (map (fn [v] (if (nil? v) 0 (Integer. v))) (% :number-injured)))})
    (plotly/layer-bar
     {:=x :$group-name
      :=y :number-injured-sum
      :=layout {:title "Number of Injuries Over Years"
                :xaxis {:title "Year"}
                :yaxis {:title "Number of Injuries"}}}))

;; ## Inuries on Pill Hill, over time, by year
(def pill-hill-crashes
  (let [crashes (-> oakland-city-crashes
                     (ds/filter #(clojure.string/includes? (or (:primary-road %)
                                                               (:secondary-road %)) "TELEGRAPH"))
                     (ds/filter (fn [row]
                                  (or (some #(clojure.string/includes? (:primary-road row) %)
                                            (keys pill-hill-intersections-of-interest))
                                      (some #(clojure.string/includes? (:secondary-road row) %)
                                            (keys pill-hill-intersections-of-interest))))))]
    (-> crashes
    (tc/map-columns :intersection-lat
                        (tc/column-names crashes #{:secondary-road})
                        (fn [secondary-road]
                           (let [match (some (fn [[k v]]
                                               (when (clojure.string/includes? secondary-road k)
                                                 v))
                                            pill-hill-intersections-of-interest)]
                             (:lat match))))
        (tc/map-columns :intersection-lng
                        (tc/column-names crashes #{:secondary-road})
                        (fn [secondary-road]
                           (let [match (some (fn [[k v]]
                                               (when (clojure.string/includes? secondary-road k)
                                                 v))
                                            pill-hill-intersections-of-interest)]
                             (:lng match)))))))

;; Line chart depicting number of crashes. Oakland is one line and Telegraph is another line
(-> oakland-city-crashes
    (ds/row-map (fn [row]
                  (let [date-time (:crash-date-time row)]
                    (assoc row
                           :year (str (.getYear date-time))))))
    (tc/dataset)
    (tc/group-by [:year])
    (tc/aggregate {:count tc/row-count})
    (plotly/layer-line
     {:=x :year
      :=y :count
      :=mark-color "purple"}))

(-> telegraph-ave-crashes
    (ds/row-map (fn [row]
                  (let [date-time (:crash-date-time row)]
                    (assoc row
                           :year (str (.getYear date-time))))))
    (tc/dataset)
    (tc/group-by [:year])
    (tc/aggregate {:count tc/row-count})
    (plotly/layer-line
     {:=x :year
      :=y :count
      :=mark-color "red"}))

(-> kono-crashes
    (ds/row-map (fn [row]
                  (let [date-time (:crash-date-time row)]
                    (assoc row
                           :year (str (.getYear date-time))))))
    (tc/dataset)
    (tc/group-by [:year])
    (tc/aggregate {:count tc/row-count})
    (plotly/layer-line
     {:=x :year
      :=y :count
      :=mark-color "green"}))

(-> pill-hill-crashes
    (ds/row-map (fn [row]
                  (let [date-time (:crash-date-time row)]
                    (assoc row
                           :year (str (.getYear date-time))))))
    (tc/dataset)
    (tc/group-by [:year])
    (tc/aggregate {:count tc/row-count})
    (plotly/layer-line
     {:=x :year
      :=y :count
      :=mark-color "blue"}))

;; combined data for both oakland and telegraph for crashes

(let [oakland-data (-> oakland-city-crashes
                       (ds/row-map (fn [row]
                                     (let [date-time (:crash-date-time row)]
                                       (assoc row
                                              :year (str (.getYear date-time))
                                              :source "Oakland"))))
                       (tc/dataset)
                       (tc/group-by [:year :source])
                       (tc/aggregate {:count tc/row-count})
                       (tc/add-column :normalized-count (fn [ds]
                                                          (tcc// (:count ds) (float (first (:count ds)))))))
      telegraph-data (-> telegraph-ave-crashes
                         (ds/row-map (fn [row]
                                       (let [date-time (:crash-date-time row)]
                                         (assoc row
                                                :year (str (.getYear date-time))
                                                :source "Telegraph"))))
                         (tc/dataset)
                         (tc/group-by [:year :source])
                         (tc/aggregate {:count tc/row-count})
                         (tc/add-column :normalized-count (fn [ds]
                                                            (tcc// (:count ds) (float (first (:count ds)))))))
      kono-data (-> kono-crashes
                         (ds/row-map (fn [row]
                                       (let [date-time (:crash-date-time row)]
                                         (assoc row
                                                :year (str (.getYear date-time))
                                                :source "Kono"))))
                         (tc/dataset)
                         (tc/group-by [:year :source])
                         (tc/aggregate {:count tc/row-count})
                         (tc/add-column :normalized-count (fn [ds]
                                                            (tcc// (:count ds) (float (first (:count ds)))))))
      pill-hill-data (-> pill-hill-crashes
                         (ds/row-map (fn [row]
                                       (let [date-time (:crash-date-time row)]
                                         (assoc row
                                                :year (str (.getYear date-time))
                                                :source "Pill Hill"))))
                         (tc/dataset)
                         (tc/group-by [:year :source])
                         (tc/aggregate {:count tc/row-count})
                         (tc/add-column :normalized-count (fn [ds]
                                                            (tcc// (:count ds) (float (first (:count ds)))))))
      combined-data (tc/concat oakland-data telegraph-data kono-data pill-hill-data)]
  (-> combined-data
      (plotly/base {:=title "Number of Crashes Over Years"})
      (plotly/layer-line
       {:=x :year
        :=y :normalized-count
        :=color :source
        :=layout {:title "Number of Crashes Over Years"
                  :xaxis {:title "Year"}
                  :yaxis {:title "Number of Crashes"}}})))

(def oakland-crashes-pedestrian-involved
  (-> oakland-city-crashes
      (tc/select-rows (fn [{:keys [pedestrian-action-desc]}]
                        (some-> pedestrian-action-desc
                                (not= "NO PEDESTRIANS INVOLVED"))))))

(def telegraph-crashes-pedestrian-involved
  (-> telegraph-ave-crashes
      (tc/select-rows (fn [{:keys [pedestrian-action-desc]}]
                        (some-> pedestrian-action-desc
                                (not= "NO PEDESTRIANS INVOLVED"))))))

;; TODO: Can we compare rates of crashes vs. rates of injuries?j

;; ## Oakland Crashes with pedestrians involved
(-> oakland-crashes-pedestrian-involved
    (ds/row-map (fn [row]
                  (let [date-time (:crash-date-time row)]
                    (assoc row
                           :year (str (.getYear date-time))))))
    (tc/dataset)
    (plotly/layer-bar
     {:=x :year
      :=y :number-injured}))

;; ## Telegraph Crashes with pedestrians involved
(-> telegraph-crashes-pedestrian-involved
    (ds/row-map (fn [row]
                  (let [date-time (:crash-date-time row)]
                    (assoc row
                           :year (str (.getYear date-time))))))
    (tc/dataset)
    (plotly/layer-bar
     {:=x :year
      :=y :number-injured}))

;; ## Killed in Oakland, over time, by year
(-> oakland-city-crashes
    (ds/row-map (fn [row]
                   (let [date-time (:crash-date-time row)]
                     (assoc row
                           :year (str (.getYear date-time))))))
    (tc/dataset)
    (plotly/layer-bar
     {:=x :year
       :=y :number-killed}))

;; ## Killed on Telegraph, over time, by year
(-> telegraph-ave-crashes
    (ds/row-map (fn [row]
                  (let [date-time (:crash-date-time row)]
                    (assoc row
                           :year (str (.getYear date-time))))))
    (tc/dataset)
    (plotly/layer-bar
     {:=x :year
      :=y :number-killed}))

;; ## What drivers are crashing into, over time, on Telegraph
;; Plotting what drivers are crashing into, over time
(-> telegraph-ave-crashes
    (ds/row-map (fn [row]
                  (let [date-time (:crash-date-time row)]
                    (assoc row
                           :year (str (.getYear date-time))))))
    (tc/dataset)
    (tc/group-by [:motor-vehicle-involved-with-desc :year ])
    (tc/aggregate {:count tc/row-count})
    ((fn [df]
       (let [years          (distinct (tc/column df :year))
             other-entities (filter some? (distinct (tc/column df :motor-vehicle-involved-with-desc)))
             data           (reduce (fn [acc typ]
                                      (assoc acc (keyword (csk/->kebab-case typ))
                                   (map (fn [year]
                                          (-> df
                                              (tc/select-rows #(and (= (:year %) year)
                                                                    (= (:motor-vehicle-involved-with-desc %) typ)))
                                              (tc/column :count)
                                              first
                                              (or 0)))
                                        years)))
                                    {:x-axis-data years}
                          other-entities)]
         (kind/echarts
          {:legend {:data (keys (dissoc data :x-axis-data))}
           :xAxis  {:type "category" :data years}
           :yAxis  {:type "value"}
           :series (map (fn [entity]
                          {:name entity
                           :type "bar"
                           :stack "total"
                           :data (get data (keyword (csk/->kebab-case entity)))})
                        (keys (dissoc data :x-axis-data)))})))))

(def oakland-crashes-with-peds-and-cyclists
  (-> oakland-city-crashes
      (tc/select-rows (fn [row]
                        (ped-and-bike-codes (:motor-vehicle-involved-with-code row))))))

(def telegraph-ave-crashes-with-peds-and-cyclists
  (-> telegraph-ave-crashes
      (tc/select-rows (fn [row]
                        (ped-and-bike-codes (:motor-vehicle-involved-with-code row))))))

(def kono-crashes-with-peds-and-cyclists
  (-> kono-crashes
      (tc/select-rows (fn [row]
                        (ped-and-bike-codes (:motor-vehicle-involved-with-code row))))))

(def pill-hill-crashes-with-peds-and-cyclists
  (-> pill-hill-crashes
      (tc/select-rows (fn [row]
                        (ped-and-bike-codes (:motor-vehicle-involved-with-code row))))))

(def oakland-city-injured-bikes-peds
  (let [collision-ids        (-> oakland-crashes-with-peds-and-cyclists
                                 (tc/select-columns :collision-id)
                                 (tc/rows)
                                 flatten
                                 set)
        all-injured-oakland (-> (load-and-combine-csvs injured-witness-passengers-csv-files)
                                 (ds/select-columns [:collision-id
                                                     :injured-wit-pass-id
                                                     :stated-age
                                                     :gender
                                                     :injured-person-type])
                                 (tc/select-rows (fn [row]
                                                   (contains? collision-ids (:collision-id row)))))

        possible-types        (-> all-injured-oakland
                                  (tc/select-columns :injured-person-type)
                                  (tc/rows)
                                  flatten
                                  set)
        oakland-with-crash-data (-> oakland-crashes-with-peds-and-cyclists
                                  (tc/select-columns [:collision-id
                                                      :crash-date-time
                                                      :motor-vehicle-involved-with-code
                                                      :motor-vehicle-involved-with-desc
                                                      :motor-vehicle-involved-with-other-desc
                                                      :number-injured
                                                      :number-killed
                                                      :lighting-description
                                                      :latitude
                                                      :longitude
                                                      :primary-road
                                                      :secondary-road]))]
    (-> all-injured-oakland
        (tc/select-rows (fn [row] (contains? #{"Pedestrian" "Bicyclist" "Other"} (:injured-person-type row))))
        (tc/inner-join oakland-with-crash-data
                       {:left  :collision-id
                        :right :collision-id}))))

(def telegraph-ave-injured-bikes-peds
  (let [collision-ids        (-> telegraph-ave-crashes-with-peds-and-cyclists
                                 (tc/select-columns :collision-id)
                                 (tc/rows)
                                 flatten
                                 set)
        all-injured-on-telegraph (-> (load-and-combine-csvs injured-witness-passengers-csv-files)
                                 (ds/select-columns [:collision-id
                                                     :injured-wit-pass-id
                                                     :stated-age
                                                     :gender
                                                     :injured-person-type])
                                 (tc/select-rows (fn [row]
                                                   (contains? collision-ids (:collision-id row)))))

        possible-types        (-> all-injured-on-telegraph
                                  (tc/select-columns :injured-person-type)
                                  (tc/rows)
                                  flatten
                                  set)
        telegraph-with-crash-data (-> telegraph-ave-crashes-with-peds-and-cyclists
                                  (tc/select-columns [:collision-id
                                                      :crash-date-time
                                                      :motor-vehicle-involved-with-code
                                                      :motor-vehicle-involved-with-desc
                                                      :motor-vehicle-involved-with-other-desc
                                                      :number-injured
                                                      :number-killed
                                                      :lighting-description
                                                      :latitude
                                                      :longitude
                                                      :primary-road
                                                      :secondary-road]))
        crashes               (-> all-injured-on-telegraph
                                  (tc/select-rows (fn [row] (contains? #{"Pedestrian" "Bicyclist" "Other"} (:injured-person-type row))))
                                  (tc/inner-join telegraph-with-crash-data
                                                 {:left  :collision-id
                                                  :right :collision-id}))]
    (-> crashes
        (tc/map-columns :intersection-lat
                        (tc/column-names crashes #{:secondary-road})
                        (fn [secondary-road]
                          (let [match (some (fn [[k v]]
                                              (when (clojure.string/includes? secondary-road k)
                                                v))
                                            telegraph-intersections-of-interest)]
                            (:lat match))))
        (tc/map-columns :intersection-lng
                        (tc/column-names crashes #{:secondary-road})
                        (fn [secondary-road]
                          (let [match (some (fn [[k v]]
                                              (when (clojure.string/includes? secondary-road k)
                                                v))
                                            telegraph-intersections-of-interest)]
                            (:lng match))))
        spy)))

(def telegraph-ave-injuries-to-heatmaps
  (mapv (fn [{:keys [intersection-lat intersection-lng row-count]}]
          [intersection-lat intersection-lng (* 1.5 row-count)])
        (-> telegraph-ave-injured-bikes-peds
            (tc/group-by [:intersection-lat :intersection-lng])
            (tc/aggregate {:row-count tc/row-count})
            (tc/rows :as-maps))))

(kind/hiccup
 [:div [:script
        {:src "https://cdn.jsdelivr.net/npm/leaflet.heat@0.2.0/dist/leaflet-heat.min.js"}]
  ['(fn [latlngs]
      [:div
       {:style {:height "500px"}
        :ref   (fn [el]
                 (let [m (-> js/L
                             (.map el)
                             (.setView (clj->js [37.821925 -122.266376])
                                       14.3))]
                   (-> js/L
                       .-tileLayer
                       (.provider "Stadia.AlidadeSmooth")
                       (.addTo m))
                   (doseq [latlng latlngs]
                     (-> js/L
                         (.marker (clj->js latlng))
                         (.addTo m)))
                   (-> js/L
                       (.heatLayer (clj->js latlngs)
                                   (clj->js {:radius 30}))
                       (.addTo m))))}])
   telegraph-ave-injuries-to-heatmaps]]
 ;; Note we need to mention the dependency:
 {:html/deps [:leaflet]})

(def kono-injured-bikes-peds
  (let [collision-ids        (-> kono-crashes-with-peds-and-cyclists
                                 (tc/select-columns :collision-id)
                                 (tc/rows)
                                 flatten
                                 set)
        all-injured-on-kono (-> (load-and-combine-csvs injured-witness-passengers-csv-files)
                                 (ds/select-columns [:collision-id
                                                     :injured-wit-pass-id
                                                     :stated-age
                                                     :gender
                                                     :injured-person-type])
                                 (tc/select-rows (fn [row]
                                                   (contains? collision-ids (:collision-id row)))))

        possible-types        (-> all-injured-on-kono
                                  (tc/select-columns :injured-person-type)
                                  (tc/rows)
                                  flatten
                                  set)
        kono-with-crash-data (-> kono-crashes-with-peds-and-cyclists
                                  (tc/select-columns [:collision-id
                                                      :crash-date-time
                                                      :motor-vehicle-involved-with-code
                                                      :motor-vehicle-involved-with-desc
                                                      :motor-vehicle-involved-with-other-desc
                                                      :number-injured
                                                      :number-killed
                                                      :lighting-description
                                                      :latitude
                                                      :longitude
                                                      :primary-road
                                                      :secondary-road]))
        crashes               (-> all-injured-on-kono
                                  (tc/select-rows (fn [row] (contains? #{"Pedestrian" "Bicyclist" "Other"} (:injured-person-type row))))
                                  (tc/inner-join kono-with-crash-data
                                                 {:left  :collision-id
                                                  :right :collision-id}))]
    (-> crashes
        (tc/map-columns :intersection-lat
                        (tc/column-names crashes #{:secondary-road})
                        (fn [secondary-road]
                          (let [match (some (fn [[k v]]
                                              (when (clojure.string/includes? secondary-road k)
                                                v))
                                            kono-intersections-of-interest)]
                            (:lat match))))
        (tc/map-columns :intersection-lng
                        (tc/column-names crashes #{:secondary-road})
                        (fn [secondary-road]
                          (let [match (some (fn [[k v]]
                                              (when (clojure.string/includes? secondary-road k)
                                                v))
                                            kono-intersections-of-interest)]
                            (:lng match)))))))

(def pill-hill-injured-bikes-peds
  (let [collision-ids        (-> pill-hill-crashes-with-peds-and-cyclists
                                 (tc/select-columns :collision-id)
                                 (tc/rows)
                                 flatten
                                 set)
        all-injured-on-pill-hill (-> (load-and-combine-csvs injured-witness-passengers-csv-files)
                                 (ds/select-columns [:collision-id
                                                     :injured-wit-pass-id
                                                     :stated-age
                                                     :gender
                                                     :injured-person-type])
                                 (tc/select-rows (fn [row]
                                                   (contains? collision-ids (:collision-id row)))))

        possible-types        (-> all-injured-on-pill-hill
                                  (tc/select-columns :injured-person-type)
                                  (tc/rows)
                                  flatten
                                  set)
        pill-hill-with-crash-data (-> pill-hill-crashes-with-peds-and-cyclists
                                  (tc/select-columns [:collision-id
                                                      :crash-date-time
                                                      :motor-vehicle-involved-with-code
                                                      :motor-vehicle-involved-with-desc
                                                      :motor-vehicle-involved-with-other-desc
                                                      :number-injured
                                                      :number-killed
                                                      :lighting-description
                                                      :latitude
                                                      :longitude
                                                      :primary-road
                                                      :secondary-road]))
        crashes               (-> all-injured-on-pill-hill
                                  (tc/select-rows (fn [row] (contains? #{"Pedestrian" "Bicyclist" "Other"} (:injured-person-type row))))
                                  (tc/inner-join pill-hill-with-crash-data
                                                 {:left  :collision-id
                                                  :right :collision-id}))]
    (-> crashes
        (tc/map-columns :intersection-lat
                        (tc/column-names crashes #{:secondary-road})
                        (fn [secondary-road]
                          (let [match (some (fn [[k v]]
                                              (when (clojure.string/includes? secondary-road k)
                                                v))
                                            pill-hill-intersections-of-interest)]
                            (:lat match))))
        (tc/map-columns :intersection-lng
                        (tc/column-names crashes #{:secondary-road})
                        (fn [secondary-road]
                          (let [match (some (fn [[k v]]
                                              (when (clojure.string/includes? secondary-road k)
                                                v))
                                            pill-hill-intersections-of-interest)]
                            (:lng match)))))))

;; normalizing injured 
(let [oakland-data (-> oakland-city-injured-bikes-peds
                       (ds/row-map (fn [row]
                                     (let [date-time (:crash-date-time row)]
                                       (assoc row
                                              :year (str (.getYear date-time))
                                              :source "Oakland"))))
                       (tc/dataset)
                       (tc/group-by [:year :source])
                       (tc/aggregate {:count tc/row-count})
                       (tc/add-column :normalized-count (fn [ds]
                                                          (tcc// (:count ds) (float (first (:count ds)))))))
      telegraph-data (-> telegraph-ave-injured-bikes-peds
                         (ds/row-map (fn [row]
                                       (let [date-time (:crash-date-time row)]
                                         (assoc row
                                                :year (str (.getYear date-time))
                                                :source "Telegraph"))))
                         (tc/dataset)
                         (tc/group-by [:year :source])
                         (tc/aggregate {:count tc/row-count})
                         (tc/add-column :normalized-count (fn [ds]
                                                            (tcc// (:count ds) (float (first (:count ds)))))))
      kono-data (-> kono-injured-bikes-peds
                         (ds/row-map (fn [row]
                                       (let [date-time (:crash-date-time row)]
                                         (assoc row
                                                :year (str (.getYear date-time))
                                                :source "Kono"))))
                         (tc/dataset)
                         (tc/group-by [:year :source])
                         (tc/aggregate {:count tc/row-count})
                         (tc/add-column :normalized-count (fn [ds]
                                                            (tcc// (:count ds) (float (first (:count ds)))))))
      pill-hill-data (-> pill-hill-injured-bikes-peds
                         (ds/row-map (fn [row]
                                       (let [date-time (:crash-date-time row)]
                                         (assoc row
                                                :year (str (.getYear date-time))
                                                :source "Pill Hill"))))
                         (tc/dataset)
                         (tc/group-by [:year :source])
                         (tc/aggregate {:count tc/row-count})
                         (tc/add-column :normalized-count (fn [ds]
                                                            (tcc// (:count ds) (float (first (:count ds)))))))
      combined-data (tc/concat oakland-data telegraph-data kono-data pill-hill-data)]
  (-> combined-data
      (plotly/base {:=title "Normalized Number of Inuries Over Years"})
      (plotly/layer-line
       {:=x :year
        :=y :normalized-count
        :=color :source})))


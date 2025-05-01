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
;; On Feb 3, 2025, Michael Burawoy was killed in the crosswalk by a speeding driver in a hit-and-run.
;; The crosswalk crosses Grande Ave, connecting the Adam's Point neighborhood to Lake Merritt:
;; the grand jewel of Oakland.

;; INSERT image of crosswalk

;; INSERT map of Grand Ave along Lake Merrit to show the landscape.

;; The City of Oakland is currently looking re-paving Grand Ave, and the local neighborhoods
;; and street safety groups are hoping that the re-paving will also include upgrades in the
;; pedestrian and bicycle infrastructure so that more neighbors are not lost on this street.

;; Let's look at Grand Ave, from Harrison to Mandana.

;; NOTE: insert map similar to this one, here:

(kind/image {:src "notebooks/images/grand-heatmap.jpg"
             :alt "Heat Map of Grand Ave, Oakland, CA"
             :caption "Grand Ave Heatmap"})

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
                                        (keys grand-intersections-of-interest)))))]
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
                             (.setView (clj->js [37.809401 -122.253160])
                                       15))]
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

;; Types of crashes since 2017
(-> grand-ave-crashes
    (tc/group-by [:motor-vehicle-involved-with-desc])
    (tc/aggregate {:count tc/row-count})
    ((fn [df]
       (let [data (map (fn [row]
                         {:name (first row)
                          :value (second row)})
                  (tc/rows df))]
         (kind/echarts
          {:title {:text "Types of Crashes on Grand Ave 2017-2024"}
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

#_(-> grand-ave-crashes-with-peds-and-cyclists
    (ds/row-map (fn [row]
                  (let [date-time (:crash-date-time row)]
                    (assoc row
                           :year (str (.getYear date-time))))))
    (tc/group-by :year)
    (tc/aggregate {:number-injured-sum #(reduce + (map (fn [v]
                                                         (if (nil? v)
                                                           0
                                                           (Integer. v)))
                                                       (% :number-injured)))})
    (plotly/layer-bar
    {:=x :$group-name
       :=y :number-injured-sum
       :=layout {:title "Number of Injuries Over Years"
                 :xaxis {:title "Year"}
                 :yaxis {:title "Number of Injuries"}}}))

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

;; TODO: Can we show a map of which intersections the injuries are happening on?

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

        possible-types       (-> all-injured-on-grand
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
                                                      :secondary-road]))]
    (-> all-injured-on-grand
        (tc/select-rows (fn [row] (contains? #{"Pedestrian" "Bicyclist" "Other"} (:injured-person-type row))))
        (tc/inner-join grand-with-crash-data
                       {:left  :collision-id
                        :right :collision-id}))))

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
;; During 2020-2022, Telegraph Ave, from 19th St to 41st St was re-worked with pedestrians, bicyclists, and bus-riders in mind.
;; This included reducing the number of lanes for cars, adding bulbouts, bike lanes, and bus loading islands.
;; We we first look at the crash data prior to 2020 and how it changed after the changes were made.

;; Using data from the [California Crash Reporting System (CCRS)](https://data.ca.gov/dataset/ccrs)


(def telegraph-intersections-of-interest
  #{"19TH" "WILLIAM" "20TH" "BERKLEY" "21ST" "22ND"
    "GRAND" "23RD" "24TH" "25TH" "SYCAMORE" "26TH"
    "MERRICMAC" "27TH" "28TH" "29TH" "30TH" "31ST"
    "32ND" "HAWTHORNE" "33RD" "34TH" "MACARTHUR" "35TH"
    "36TH" "37TH" "38TH" "APGAR" "39TH" "40TH" "41ST"})


(def oakland-city-crashes
  (-> (load-and-combine-csvs crash-csv-files)
      (ds/select-columns [:collision-id
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

(def telegraph-ave-crashes
  (-> oakland-city-crashes
      (ds/filter #(clojure.string/includes? (or (:primary-road %)
                                                (:secondary-road %)) "TELEGRAPH"))
      (ds/filter (fn [row]
                   (or (some #(clojure.string/includes? (:primary-road row) %)
                             telegraph-intersections-of-interest)
                       (some #(clojure.string/includes? (:secondary-road row) %)
                             telegraph-intersections-of-interest))))))

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
      combined-data (tc/concat oakland-data telegraph-data)]
  (plotly/layer-line
   combined-data
   {:=x :year
    :=y :normalized-count
    :=color :source
    :=layout {:title "Number of Crashes Over Years"
              :xaxis {:title "Year"}
              :yaxis {:title "Number of Crashes"}}}))

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


;; # Grand Ave Crash Data

(def grand-intersections-of-interest
  #{"HARRISON" "BAY" "PARK VIEW" "BELLEVUE"
    "LENOX" "LEE" "PERKINS" "ELLITA" "STATEN"
    "EUCLID" "EMBARCADERO" "MACARTHUR" "LAKE PARK"
    "SANTA CLARA" "ELWOOD" "MANDANA"})

#_(def grand-ave-crashes
  (-> (load-and-combine-csvs crash-csv-files)
      (ds/select-columns [:collision-id
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
                          :secondary-road])
      (ds/filter #(clojure.string/includes? (or (:primary-road %)
                                                (:secondary-road %)) "GRAND"))
      (ds/filter (fn [row]
                   (or (some #(clojure.string/includes? (:primary-road row) %)
                             grand-intersections-of-interest)
                       (some #(clojure.string/includes? (:secondary-road row) %)
                             grand-intersections-of-interest))))))

(-> grand-ave-crashes
    (tc/dataset)
    (plotly/layer-bar
     {:=x :crash-date-time
      :=y :number-injured}))

(-> grand-ave-crashes
    (ds/row-map (fn [row]
                  (let [date-time (:crash-date-time row)]
                    (assoc row
                           :month-year (str (.getYear date-time) "-" (.getMonthValue date-time))))))
    (tc/dataset)
    (plotly/layer-bar
     {:=x :month-year
      :=y :number-injured}))

(-> grand-ave-crashes
    (ds/row-map (fn [row]
                  (let [date-time (:crash-date-time row)]
                    (assoc row
                           :year (str (.getYear date-time))))))
    (tc/dataset)
    (plotly/layer-bar
     {:=x :year
      :=y :number-injured}))

(-> grand-ave-crashes
    (ds/row-map (fn [row]
                  (let [date-time (:crash-date-time row)]
                    (assoc row
                           :year (str (.getYear date-time))))))
    (tc/dataset)
    (plotly/layer-bar
     {:=x :year
      :=y :number-killed}))

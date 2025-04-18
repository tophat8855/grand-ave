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


;; # Grand Ave, Oakland, CA
;; On Feb 3, 2025, Michael Burawoy was killed in the crosswalk by a speeding driver in a hit-and-run.
;; The crosswalk crosses Grande Ave, connecting the Adam's Point neighborhood to Lake Merritt:
;; the grand jewel of Oakland.

;; The City of Oakland is currently looking re-paving Grand Ave, and the local neighborhoods
;; and street safety groups are hoping that the re-paving will also include upgrades in the
;; pedestrian and bicycle infrastructure so that more neighbors are not lost on this street.

;; Let's look at Grand Ave, from Harrison to Mandana.

;; NOTE: insert map similar to this one, here:

(kind/image {:src "notebooks/images/grand-heatmap.jpg"
             :alt "Heat Map of Grand Ave, Oakland, CA"
             :caption "Grand Ave Heatmap"})


(def grand-intersections-of-interest
  {"HARRISON"    {:lat 37.8109389 :long -122.2648802}
   "BAY"         {:lat 37.8105517 :long -122.2632057}
   "PARK VIEW"   {:lat 37.8097245 :long -122.2607476}
   "BELLEVUE"    {:lat 37.8097757 :long -122.2620034}
   "LENOX"       {:lat 37.8092907 :long -122.2610871}
   "LEE"         {:lat 37.8091254 :long -122.2585415}
   "PERKINS"     {:lat 37.8091058 :long -122.2574318}
   "ELLITA"      {:lat 37.8089329 :long -122.2575731}
   "STATEN"      {:lat 37.8087855 :long -122.2564191}
   "EUCLID"      {:lat 37.8086043 :long -122.2542574}
   "EMBARCADERO" {:lat 37.8093863 :long -122.25235}
   "MACARTHUR"   {:lat 37.8101499 :long -122.2513041}
   "LAKE PARK"   {:lat 37.8108553 :long -122.249266}
   "SANTA CLARA" {:lat 37.8118568 :long -122.2503111}
   "ELWOOD"      {:lat 37.8136874 :long -122.2491843}
   "MANDANA"     {:lat 37.8142519 :long -122.2488258}})

(def grand-ave-crashes
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
      (tc/select-rows #(clojure.string/includes? (or (:primary-road %)
                                                (:secondary-road %)) "GRAND"))
      (tc/select-rows (fn [row]
                        (or (some #(clojure.string/includes? (:primary-road row) %)
                                  (keys grand-intersections-of-interest))
                            (some #(clojure.string/includes? (:secondary-road row) %)
                                  (keys grand-intersections-of-interest)))))
      #_  (ds/group-by-column :secondary-road)
      ))


;; # Telegraph Ave Crash Data
;; During 2020-2022, Telegraph Ave, from 19th St to 41st St was re-worked with pedestrians, bicyclists, and bus-riders in mind.
;; This included reducing the number of lanes for cars, adding bulbouts, bike lanes, and bus loading islands.
;; We we first look at the crash data prior to 2020 and how it changed after the changes were made.

;; Using data from the [California Crash Reporting System (CCRS)](https://data.ca.gov/dataset/ccrs)

^{:kindly/hide-code true}
(def crash-csv-files
  [#_"notebooks/datasets/2015crashes.csv"
   "notebooks/datasets/2016crashes.csv"
   "notebooks/datasets/2017crashes.csv"
   "notebooks/datasets/2018crashes.csv"
   "notebooks/datasets/2019crashes.csv"
   "notebooks/datasets/2020crashes.csv"
   "notebooks/datasets/2021crashes.csv"
   "notebooks/datasets/2022crashes.csv"
   "notebooks/datasets/2023crashes.csv"
   "notebooks/datasets/2024crashes.csv"
   #_"notebooks/datasets/2025crashes.csv"])

^{:kindly/hide-code true}
(def parties-csv-files
  [#_"notebooks/datasets/2015parties.csv"
   "notebooks/datasets/2016parties.csv"
   "notebooks/datasets/2017parties.csv"
   "notebooks/datasets/2018parties.csv"
   "notebooks/datasets/2019parties.csv"
   "notebooks/datasets/2020parties.csv"
   "notebooks/datasets/2021parties.csv"
   "notebooks/datasets/2022parties.csv"
   "notebooks/datasets/2023parties.csv"
   "notebooks/datasets/2024parties.csv"
   #_"notebooks/datasets/2025parties.csv"])

^{:kindly/hide-code true}
(def injured-witness-passengers-csv-files
  [#_"notebooks/datasets/2015injuredwitnesspassengers.csv"
   "notebooks/datasets/2016injuredwitnesspassengers.csv"
   "notebooks/datasets/2017injuredwitnesspassengers.csv"
   "notebooks/datasets/2018injuredwitnesspassengers.csv"
   "notebooks/datasets/2019injuredwitnesspassengers.csv"
   "notebooks/datasets/2020injuredwitnesspassengers.csv"
   "notebooks/datasets/2021injuredwitnesspassengers.csv"
   "notebooks/datasets/2022injuredwitnesspassengers.csv"
   "notebooks/datasets/2023injuredwitnesspassengers.csv"
   "notebooks/datasets/2024injuredwitnesspassengers.csv"
  #_ "notebooks/datasets/2025injuredwitnesspassengers.csv"])

(def telegraph-intersections-of-interest
  #{"19TH" "WILLIAM" "20TH" "BERKLEY" "21ST" "22ND"
    "GRAND" "23RD" "24TH" "25TH" "SYCAMORE" "26TH"
    "MERRICMAC" "27TH" "28TH" "29TH" "30TH" "31ST"
    "32ND" "HAWTHORNE" "33RD" "34TH" "MACARTHUR" "35TH"
    "36TH" "37TH" "38TH" "APGAR" "39TH" "40TH" "41ST"})

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

(def grand-crashes-pedestrian-involved
  (-> grand-ave-crashes
      (ds/filter (fn [row]
                   (not (or (nil? (:pedestrian-action-desc row))
                            (= "NO PEDESTRIANS INVOLVED" (:pedestiran-action-desc row))))))))

(-> grand-crashes-pedestrian-involved
    (ds/row-map (fn [row]
                  (let [date-time (:crash-date-time row)]
                    (assoc row
                           :year (str (.getYear date-time))))))
    (tc/dataset)
    (plotly/layer-bar
     {:=x :year
      :=y :number-injured}))

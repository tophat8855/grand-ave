^{:kindly/hide-code true
  :kindly/kind      :kind/hiccup}
(ns index
  (:require [camel-snake-kebab.core :as csk]
            [scicloj.kindly.v4.kind :as kind]
            [scicloj.tableplot.v1.hanami :as hanami]
            [tablecloth.api :as tc]
            [tech.v3.dataset :as ds]))

^{:kindly/hide-code true
  :kindly/kind      :kind/hiccup}
(defn spy [x]
  (println x)
  x)

;; # Grand Ave Crash Data

;; Using data from the [California Crash Reporting System (CCRS)](https://data.ca.gov/dataset/ccrs)

(def intersections-of-interest
  #{"HARRISON" "BAY" "PARK VIEW" "BELLEVUE"
    "LENOX" "LEE" "PERKINS" "ELLITA" "STATEN"
    "EUCLID" "EMBARCADERO" "MACARTHUR" "LAKE PARK"
    "SANTA CLARA" "ELWOOD" "MANDANA"})

(def csv-files ["notebooks/datasets/2015crashes.csv"
                "notebooks/datasets/2016crashes.csv"
                "notebooks/datasets/2017crashes.csv"
                "notebooks/datasets/2018crashes.csv"
                "notebooks/datasets/2019crashes.csv"
                "notebooks/datasets/2020crashes.csv"
                "notebooks/datasets/2021crashes.csv"
                "notebooks/datasets/2022crashes.csv"
                "notebooks/datasets/2023crashes.csv"
                "notebooks/datasets/2024crashes.csv"
                "notebooks/datasets/2025crashes.csv"])

(defn load-and-combine-csvs [file-paths]
  (let [datasets (map #(ds/->dataset % {:key-fn    csk/->kebab-case-keyword
                                        :parser-fn {:collision-id       :integer
                                                    :ncic-code          :integer
                                                    :is-highway-related :boolean
                                                    :is-tow-away        :boolean
                                                    :number-injured     :integer
                                                    :number-killed      :integer}})
                      file-paths)]
    (apply ds/concat datasets)))

(-> (load-and-combine-csvs csv-files)
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
                           intersections-of-interest)
                     (some #(clojure.string/includes? (:secondary-road row) %)
                           intersections-of-interest))))
    (tc/dataset))

#_(-> grand-ave-crashes
    (hanami/plot hanami/line-chart
                 {:x :crash-date-time
                  :y :number-injured
                  :color :collision-type-description
                  :title "Injuries by Crash Type"
                  :width 800
                  :height 400}))

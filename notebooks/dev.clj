(ns dev
  (:require [camel-snake-kebab.core :as csk]
            [scicloj.clay.v2.api :as clay]
            [tech.v3.dataset :as ds]))

(defn spy [x]
  (clojure.pprint/pprint x)
  x)

(clay/make! {:format              [:quarto :html]
             :base-source-path    "notebooks"
             :source-path         ["index.clj"]
             :base-target-path    "docs"
             :book                {:title "Grand Ave, Oakland"}
             :clean-up-target-dir true})

;; The following code is used to filter the datasets from the original crash data files to only Oakland crashes.
;; This allows us to have lessdata to work with and to focus on the data that is relevant to our analysis.

(def crash-csv-files
  "CSV files containing crash data"
  ["raw-datasets/2015crashes.csv"
   "raw-datasets/2016crashes.csv"
   "raw-datasets/2017crashes.csv"
   "raw-datasets/2018crashes.csv"
   "raw-datasets/2019crashes.csv"
   "raw-datasets/2020crashes.csv"
   "raw-datasets/2021crashes.csv"
   "raw-datasets/2022crashes.csv"
   "raw-datasets/2023crashes.csv"
   "raw-datasets/2024crashes.csv"
   "raw-datasets/2025crashes.csv"])

(def parties-csv-files
  "CSV files containing affected party data"
  ["raw-datasets/2015parties.csv"
   "raw-datasets/2016parties.csv"
   "raw-datasets/2017parties.csv"
   "raw-datasets/2018parties.csv"
   "raw-datasets/2019parties.csv"
   "raw-datasets/2020parties.csv"
   "raw-datasets/2021parties.csv"
   "raw-datasets/2022parties.csv"
   "raw-datasets/2023parties.csv"
   "raw-datasets/2024parties.csv"
   "raw-datasets/2025parties.csv"])

(def injured-witness-passengers-csv-files
  "CSV files containing injured witness passengers data"
  ["raw-datasets/2015injuredwitnesspassengers.csv"
   "raw-datasets/2016injuredwitnesspassengers.csv"
   "raw-datasets/2017injuredwitnesspassengers.csv"
   "raw-datasets/2018injuredwitnesspassengers.csv"
   "raw-datasets/2019injuredwitnesspassengers.csv"
   "raw-datasets/2020injuredwitnesspassengers.csv"
   "raw-datasets/2021injuredwitnesspassengers.csv"
   "raw-datasets/2022injuredwitnesspassengers.csv"
   "raw-datasets/2023injuredwitnesspassengers.csv"
   "raw-datasets/2024injuredwitnesspassengers.csv"
   "raw-datasets/2025injuredwitnesspassengers.csv"])

(defn filter-and-write-csv [file-path]
  (-> file-path
      (ds/->dataset {:key-fn csk/->kebab-case-keyword})
      (ds/filter-column
       :city-name #{"Oakland"})
      (ds/write! (str "notebooks/datasets/" (last (clojure.string/split file-path #"/"))))))

(doseq [file crash-csv-files]
  (filter-and-write-csv file))

(defn get-oakland-collision-ids []
  (->> crash-csv-files
       (map #(ds/->dataset % {:key-fn csk/->kebab-case-keyword}))
       (map #(ds/filter-column % :city-name #{"Oakland"}))
       (map #(ds/select-columns % [:collision-id]))
       (mapcat ds/rows)
       (mapcat vals)
       (set)))

(defn filter-related-data [file-path collision-ids]
  (-> file-path
      (ds/->dataset {:key-fn csk/->kebab-case-keyword})
      spy
      (ds/filter-column
       :collision-id (spy collision-ids))
      (ds/write! (str "notebooks/datasets/" (last (clojure.string/split file-path #"/"))))))

(defn process-related-files [csv-files collision-ids]
  (doseq [file csv-files]
    (filter-related-data file collision-ids)))

(let [oakland-collision-ids (get-oakland-collision-ids)]
  (process-related-files parties-csv-files oakland-collision-ids)
  (process-related-files injured-witness-passengers-csv-files oakland-collision-ids))

(ns dev
  (:require [camel-snake-kebab.core :as csk]
            [scicloj.clay.v2.api :as clay]
            [tech.v3.dataset :as ds]))

(clay/make! {:format              [:quarto :html]
             :base-source-path    "notebooks"
             :source-path         ["index.clj"]
             :base-target-path    "docs"
             :book                {:title "Grand Ave, Oakland"}
             :clean-up-target-dir true})

(def csv-files ["raw-datasets/2015crashes.csv"
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

(defn filter-and-write-csv [file-path]
  (-> file-path
      (ds/->dataset {:key-fn csk/->kebab-case-keyword})
      (ds/filter-column
       :city-name #{"Oakland"})
      (ds/write! (str "notebooks/datasets/" (last (clojure.string/split file-path #"/"))))))

(doseq [file csv-files]
  (filter-and-write-csv file))

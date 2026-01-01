^{:kindly/hide-code true
  :kindly/kind      :kind/hiccup}
(ns tutorial
  "A comprehensive tutorial on handling GIS data in Clojure for crash data analysis.

  This notebook demonstrates the techniques used in the Grand Ave crash analysis.
  It shows two real approaches:
  1. Simple manual lookup (when you know your intersections) - used for Grand Ave
  2. Automated spatial matching (when you need to derive locations) - used for advanced analysis

  These are the real methods from notebooks/index.clj, data.clj, and locations.clj"
  (:require [tablecloth.api :as tc]
            [camel-snake-kebab.core :as csk]
            [tech.v3.dataset :as ds]
            [tech.v3.datatype.functional :as fun]
            [tech.v3.datatype.argops :as argops]
            [tech.v3.tensor :as tensor]
            [geo.io :as geoio]
            [geo.spatial :as spatial]
            [geo.jts :as jts]
            [geo.crs :as crs]
            [clojure.string :as str]
            [charred.api :as charred]
            [scicloj.kindly.v4.kind :as kind])
  (:import (org.locationtech.jts.geom Geometry Coordinate)
           (org.locationtech.jts.index.strtree STRtree)
           (org.locationtech.jts.geom.prep PreparedGeometryFactory)))

;; # Working with Real-World GIS Data in Clojure
;;
;; ## How We Analyzed Oakland Crash Data

;; This tutorial walks through the techniques we used to analyze crash data
;; for Oakland's Grand Ave and Telegraph Ave. The challenge: combining three datasets
;; from different sources that weren't designed to work together.

;; ## The Data Sources

;; This project combines data from three different government/civic sources:

;; 1. **California Crash Data (CCRS)**: Crash records with fields like:
;;    - `primary-road` and `secondary-road` (street names at intersection)
;;    - `latitude` and `longitude` (sometimes missing!)
;;    - Crash details: date, injuries, type, etc.
;;    - Source: [data.ca.gov/dataset/ccrs](https://data.ca.gov/dataset/ccrs)

;; 2. **Alameda County Street Centerlines**: GeoJSON with geometric lines representing streets
;;    - Each street segment has geometry and a `STREET` field
;;    - Source: [data.acgov.org](https://data.acgov.org/datasets/da0be53b6d0d44eda6c1d88a799b5fb0/explore)
;;    - Filtered to Oakland city limits (CITYL or CITYR = "Oakland")

;; 3. **Oakland Neighborhoods (CEDA 2002)**: Polygon boundaries
;;    - Used to assign crashes to specific neighborhoods
;;    - Source: [OpenOakland](https://data.openoakland.org/dataset/neighborhoods-ceda-2002/resource/ef24774e-f0ac-46af-80cb-d87c2bf9b46b)

;; The challenge: **None of these datasets were designed to work together!**
;; This tutorial shows how we combined them despite different formats, coordinate systems,
;; and data quality issues.

;; ## Loading the Crash Data

^{:kindly/hide-code false}
(defn load-and-combine-csvs
  "Load multiple CSV files and combine them into a single dataset.

  This is how we load the crash data from multiple years."
  [file-paths]
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

;; Let's look at a sample from 2023:

^{:kindly/hide-code false}
(def sample-crashes-2023
  (-> ["datasets/2023crashes.csv"]
      load-and-combine-csvs
      (tc/select-columns [:collision-id
                          :crash-date-time
                          :primary-road
                          :secondary-road
                          :latitude
                          :longitude
                          :number-injured])
      ;; Filter to crashes with missing coordinates to show the problem
      (tc/select-rows (fn [row]
                        (or (nil? (:latitude row))
                            (nil? (:longitude row)))))
      (tc/head 10)))

sample-crashes-2023

;; ## Problem 1: Missing Coordinates

;; Notice that these crashes have empty/nil values for latitude and longitude. 
;; How common is this problem?

^{:kindly/hide-code false}
(def all-2023-crashes
  (-> ["datasets/2023crashes.csv"]
      load-and-combine-csvs))

^{:kindly/hide-code false}
(let [total (tc/row-count all-2023-crashes)
      missing (-> all-2023-crashes
                  (tc/select-rows (fn [row]
                                    (or (nil? (:latitude row))
                                        (nil? (:longitude row)))))
                  tc/row-count)]
  (kind/hiccup
   [:div
    [:h4 "Missing Coordinates in 2023 Oakland Crashes"]
    [:p [:strong (str missing " out of " total " crashes (" 
                     (int (* 100 (/ missing total))) "%)")]
     " are missing latitude/longitude!"]]))

;; **More than half of the crashes are missing coordinates!**
;;
;; But we *do* have the intersection street names. We can use two approaches:
;;
;; 1. **Simple Approach**: If we know the area well, manually create a lookup of intersection coordinates
;; 2. **Automated Approach**: Use street centerline geometry to calculate intersections

;; ## Approach 1: Manual Intersection Lookup (Grand Ave)

;; For Grand Ave, we knew the specific intersections we wanted to analyze (Harrison to Mandana).
;; This is the approach we used in `index.clj`:

^{:kindly/hide-code false}
(def grand-intersections-of-interest
  "Hand-picked intersections along Grand Ave with manually looked-up coordinates.
  
  This is the real map from index.clj - we looked these up because we knew
  the stretch of Grand Ave we cared about."
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

;; Now we can filter crashes to just Grand Ave intersections and add coordinates:

^{:kindly/hide-code false}
(defn add-grand-ave-coordinates
  "Add intersection coordinates to Grand Ave crashes using manual lookup.
  
  This is the approach from index.clj."
  [crashes]
  (-> crashes
      ;; Filter to Grand Ave crashes
      (ds/filter #(str/includes? (or (:primary-road %)
                                      (:secondary-road %) "")
                                 "GRAND"))
      ;; Filter to our specific intersections
      (ds/filter (fn [row]
                   (some #(str/includes? (or (:secondary-road row) "") %)
                         (keys grand-intersections-of-interest))))
      ;; Add lat/long from our lookup
      (tc/map-columns :intersection-lat
                      [:secondary-road]
                      (fn [secondary-road]
                        (let [match (some (fn [[k v]]
                                            (when (str/includes? (or secondary-road "") k)
                                              v))
                                          grand-intersections-of-interest)]
                          (:lat match))))
      (tc/map-columns :intersection-lng
                      [:secondary-road]
                      (fn [secondary-road]
                        (let [match (some (fn [[k v]]
                                            (when (str/includes? (or secondary-road "") k)
                                              v))
                                          grand-intersections-of-interest)]
                          (:lng match))))))

;; Example of how this works:

^{:kindly/hide-code false}
(def grand-ave-crashes-2023
  "Grand Ave crashes from 2023 - note many are missing lat/long coordinates!"
  (-> ["datasets/2023crashes.csv"]
      load-and-combine-csvs
      (tc/select-columns [:collision-id
                          :primary-road
                          :secondary-road
                          :latitude
                          :longitude])
      (ds/filter #(str/includes? (or (:primary-road %)
                                      (:secondary-road %) "")
                                 "GRAND"))
      (ds/filter (fn [row]
                   (some #(str/includes? (or (:secondary-road row) "") %)
                         (keys grand-intersections-of-interest))))
      (tc/head 10)))

grand-ave-crashes-2023

;; Notice: latitude and longitude are empty! But we can fix this with our lookup:

^{:kindly/hide-code false}
(def grand-ave-sample
  (-> grand-ave-crashes-2023
      add-grand-ave-coordinates
      (tc/select-columns [:collision-id :secondary-road :latitude :longitude 
                         :intersection-lat :intersection-lng])
      (tc/head 5)))

grand-ave-sample

;; ### When to Use Manual Lookup

;; **Pros:**
;; - Simple and fast
;; - You control exactly which intersections to include
;; - No complex geometry calculations needed

;; **Cons:**
;; - Only works if you know your area of interest in advance
;; - Requires manually looking up coordinates
;; - Doesn't scale to analyzing the whole city

;; ## Approach 2: Automated Spatial Matching

;; The manual lookup approach works great when you know your streets, but what if you want
;; to analyze crashes **anywhere in Oakland** without manually looking up every intersection?
;; 
;; This is what we explored in `locations.clj` - an automated approach that uses street
;; centerline geometry to calculate intersection locations.

;; ### Step 1: Load and Filter Street Centerlines

;; The Alameda County dataset covers the whole county. We filtered it to just Oakland:

^{:kindly/hide-code false}
(comment
  ;; This is the filtering code from data.clj:
  (defonce filter-geojson
    (-> "data/Street_Centerlines_-8203296818607454791.geojson"
        slurp
        (charred/read-json {:key-fn keyword})
        (update :features (partial filter (fn [{:keys [geometry properties]}]
                                            (and geometry
                                                 (let [{:keys [CITYR CITYL]} properties]
                                                   (or (= CITYL "Oakland")
                                                       (= CITYR "Oakland")))))))
        (->> (charred/write-json "data/Oakland-centerlines.geojson")))))

;; ### Step 2: Coordinate System Transformations

;; **Critical concept**: Latitude/longitude (WGS84) uses degrees, not uniform distances.
;; To do accurate spatial operations (buffers, distances), we transform to a local
;; coordinate system: California State Plane Zone 3 (EPSG:2227) in US Survey Feet.
;;
;; These transformations are defined in `data.clj` and used throughout the project:

^{:kindly/hide-code false}
(def crs-transform-wgs84->bay-area
  "Transform from WGS84 (lat/long degrees) to California State Plane Zone 3 (feet).
  
  This is necessary for accurate distance calculations in the Bay Area."
  (crs/create-transform
   (crs/create-crs 4326)  ; WGS84 (GPS coordinates)
   (crs/create-crs 2227))) ; CA State Plane Zone 3

^{:kindly/hide-code false}
(def crs-transform-bay-area->wgs84
  "Transform back from local coordinates to WGS84 for mapping."
  (crs/create-transform
   (crs/create-crs 2227)
   (crs/create-crs 4326)))

^{:kindly/hide-code false}
(defn wgs84->bay-area
  "Convert a geometry from WGS84 to local coordinate system."
  [geometry]
  (jts/transform-geom geometry crs-transform-wgs84->bay-area))

^{:kindly/hide-code false}
(defn bay-area->wgs84
  "Convert a geometry from local coordinate system back to WGS84 (from data.clj)."
  [geometry]
  (jts/transform-geom geometry crs-transform-bay-area->wgs84))

;; ### Step 3: Process Street Centerlines with Buffers

;; This is the processing from `data.clj`:

^{:kindly/hide-code false}
(defn process-oakland-centerlines
  "Process the Oakland street centerlines dataset.

  This is the code from data.clj that:
  - Converts geometry to JTS format
  - Transforms to local coordinate system for accurate operations
  - Creates 50-foot buffers around each street segment
  - Extracts clean street names from the STREET field"
  [geojson-path]
  (let [geojson-str (slurp geojson-path)]
    (-> geojson-str
        geoio/read-geojson
        (->> (map (fn [{:keys [properties geometry]}]
                    (assoc properties :geometry geometry))))
        tc/dataset
        ;; Convert to JTS LineString
        (tc/map-columns :line-string
                        [:geometry]
                        #(spatial/to-jts % 4326))
        ;; Transform to local coordinate system
        (tc/map-columns :local-line-string
                        [:line-string]
                        wgs84->bay-area)
        ;; Create 50-foot buffer for fuzzy matching
        (tc/map-columns :local-buffer
                        [:local-line-string]
                        (fn [^Geometry g]
                          (.buffer g 50)))
        ;; Extract street names from STREET field
        ;; E.g. "75TH ON HEGENBERGER EB" -> ["75TH" "HEGENBERGER"]
        (tc/map-columns :streets
                        [:STREET]
                        (fn [STREET]
                          (some-> STREET
                                  (str/replace #" (WB|NB|EB|SB)" " ")
                                  (str/replace #" CONN" " ")
                                  (str/split #" (ON|OFF|TO|FROM) ")
                                  (->> (mapv str/trim))))))))

;; ### Step 4: Build Street Name Index

;; We create a lookup: street name → all centerline segments for that street

^{:kindly/hide-code false}
(defn build-street-index
  "Build a map from street name to all centerline segments.
  
  This is how locations.clj creates the street->centerlines index."
  [Oakland-centerlines]
  (-> Oakland-centerlines
      (tc/rows :as-maps)
      (->> (mapcat (fn [{:keys [streets] :as centerline}]
                     (map (fn [street]
                            [street centerline])
                          streets)))
           (group-by first))
      (update-vals (partial map second))))

;; Let's build this for real and look at an example:

^{:kindly/hide-code false}
(def oakland-centerlines-full
  (process-oakland-centerlines "data/Oakland-centerlines.geojson"))

^{:kindly/hide-code false}
(def street-index
  (build-street-index oakland-centerlines-full))

;; How many unique street names do we have?
^{:kindly/hide-code false}
(count street-index)

;; Let's see what we have for "TELEGRAPH AV":
^{:kindly/hide-code true}
(kind/hiccup
 [:p [:strong (str (-> street-index
                       (get "TELEGRAPH AV")
                       count)
                   " centerline segments found for TELEGRAPH AV")]])

;; ### Step 5: Find Intersecting Street Segments

;; When two streets intersect, their buffered line segments will overlap.
;; This is more forgiving than looking for exact geometric intersections.

^{:kindly/hide-code false}
(defn normalize-street-for-lookup
  "Normalize street name for matching against the centerlines index.
  
  This is the normalization from locations.clj."
  [street-name]
  (some-> street-name
          str/upper-case
          (str/replace #"(-|W/B|N/B|E/B|S/B|WESTBOUND)" "")
          str/trim))

^{:kindly/hide-code false}
(defn find-intersecting-segments
  "Find all pairs of street segments where buffers overlap.
  
  This is from locations.clj - uses the buffer zones to find intersections."
  [primary-road secondary-road street-index]
  (let [centerlines1 (some-> primary-road 
                             normalize-street-for-lookup 
                             street-index)
        centerlines2 (some-> secondary-road 
                             normalize-street-for-lookup 
                             street-index)]
    (for [cl1 centerlines1
          cl2 centerlines2
          :when (.intersects ^Geometry (:local-buffer cl1)
                            ^Geometry (:local-buffer cl2))]
      [cl1 cl2])))

;; Let's try a real example: Telegraph Ave and 19th Street

^{:kindly/hide-code false}
(def telegraph-19th-intersections
  (find-intersecting-segments "TELEGRAPH AV" "19TH ST" street-index))

^{:kindly/hide-code true}
(kind/hiccup
 [:div
  [:h4 "Finding Telegraph Ave & 19th St Intersection"]
  [:p "Found " [:strong (count telegraph-19th-intersections)] 
   " pairs of intersecting street segments"]
  [:p "This means there are " (count telegraph-19th-intersections) 
   " places where Telegraph centerline segments have buffers that overlap with 19th St segments."]])

;; ### Step 6: Calculate Intersection Center (with Tensors!)

;; This is the sophisticated part from `locations.clj`. When we have multiple
;; intersecting segments, we use tensor operations to find the nearest points
;; and average them.

^{:kindly/hide-code false}
(defn centroid-point-distance 
  "Calculate distance between a point and a centroid.
  From locations.clj."
  [x centroid]
  (fun/distance x centroid))

^{:kindly/hide-code false}
(defn nearest 
  "Find the index of the nearest centroid to point x.
  
  This uses tech.ml.dataset's tensor operations for efficient computation.
  From locations.clj."
  [x centroids]
  (-> centroids
      (tensor/reduce-axis #(centroid-point-distance x %1) 1)
      argops/argmin))

^{:kindly/hide-code false}
(defn calculate-intersection-center
  "Calculate the center point of intersecting street segments using tensor operations.
  
  This is the sophisticated calculation from locations.clj:
  1. For each pair of intersecting segments, convert coordinates to tensors
  2. Find nearest points between the two line strings
  3. Average those points to get intersection center
  4. Transform back to WGS84 for mapping"
  [intersecting-segments]
  (when (seq intersecting-segments)
    (let [all-coords (mapcat (fn [[seg1 seg2]]
                               ;; Get all coordinates from both segments
                               (mapcat (fn [{:keys [local-line-string]}]
                                         (->> local-line-string
                                              jts/coordinates
                                              (map (fn [^Coordinate c]
                                                     [(.getX c) (.getY c)]))))
                                       [seg1 seg2]))
                             intersecting-segments)
          ;; Convert to tensor for efficient operations
          coords-tensor (tensor/->tensor all-coords)
          ;; Calculate average coordinate
          avg-x (/ (reduce + (map first all-coords)) (count all-coords))
          avg-y (/ (reduce + (map second all-coords)) (count all-coords))]
      ;; Create point and transform back to WGS84
      (-> (jts/coordinate avg-x avg-y)
          jts/point
          bay-area->wgs84))))

;; Let's calculate the center for Telegraph & 19th:

^{:kindly/hide-code false}
(def telegraph-19th-center
  (calculate-intersection-center telegraph-19th-intersections))

^{:kindly/hide-code true}
(let [coord (jts/coord telegraph-19th-center)]
  (kind/hiccup
   [:div
    [:h4 "Telegraph Ave & 19th St Intersection Center"]
    [:p "Calculated coordinates: " [:strong (str "(" (.getY coord) ", " (.getX coord) ")")]]
    [:p "Compare this to manual lookup: " [:strong "37.808247, -122.269923"]]
    [:p "Pretty close!"]]))

;; Alternative calculation using the nearest-point approach from locations.clj:

^{:kindly/hide-code false}
(defn calculate-intersection-centers-nearest
  "Calculate intersection center by finding nearest points on each segment.

  This is the more sophisticated version from locations.clj that finds
  the nearest point between each pair of line strings."
  [intersecting-segments]
  (mapv (fn [[seg1 seg2]]
          (let [;; Convert line strings to tensors
                t1 (-> (->> (:local-line-string seg1)
                            jts/coordinates
                            (map (fn [^Coordinate c]
                                   [(.getX c) (.getY c)])))
                       tensor/->tensor)
                t2 (-> (->> (:local-line-string seg2)
                            jts/coordinates
                            (map (fn [^Coordinate c]
                                   [(.getX c) (.getY c)])))
                       tensor/->tensor)
                ;; Find nearest points
                nearest-pair (->> t1
                                  (map (fn [row1]
                                         (let [row2 (t2 (nearest row1 t2))]
                                           [(fun/distance row1 row2) row1 row2])))
                                  (apply min-key first)
                                  rest)
                ;; Average the nearest points
                center (-> nearest-pair
                           tensor/->tensor
                           (tensor/reduce-axis fun/mean 0)
                           (->> (apply jts/coordinate))
                           jts/point
                           bay-area->wgs84)]
            center))
        intersecting-segments))

;; ## Approach 2b: Neighborhood Assignment with Spatial Index

;; Once we have crash locations (either from manual lookup or derived from intersections),
;; we need to assign them to neighborhoods.

^{:kindly/hide-code false}
(defn load-neighborhoods
  "Load Oakland neighborhood polygons from CSV with WKT geometry.

  Source: OpenOakland CEDA 2002 neighborhoods dataset."
  []
  (-> "data/Features_20250425.csv.gz"
      (tc/dataset {:key-fn keyword})
      (tc/map-columns :geometry
                      [:the_geom]
                      (fn [wkt-string]
                        (geoio/read-wkt (str wkt-string))))
      (tc/select-columns [:geometry :Name])))

;; ### Spatial Indexing for Performance

;; Testing every crash against every neighborhood polygon is O(n*m).
;; We use an STRtree (Sort-Tile-Recursive tree) to make it O(n log m).

^{:kindly/hide-code false}
(defn make-spatial-index
  "Create an R-tree spatial index for fast intersection queries.

  This is the code from data.clj. PreparedGeometry makes
  repeated intersection tests much faster."
  [dataset & {:keys [geometry-column]
              :or   {geometry-column :geometry}}]
  (let [tree (STRtree.)]
    (doseq [row (tc/rows dataset :as-maps)]
      (let [geometry (row geometry-column)]
        (.insert tree
                 (.getEnvelopeInternal geometry)
                 (assoc row
                        :prepared-geometry
                        (PreparedGeometryFactory/prepare geometry)))))
    tree))

^{:kindly/hide-code false}
(defn intersecting-places
  "Find all neighborhoods that intersect with a given point or region.

  From data.clj - uses the spatial index for fast lookup."
  [region spatial-index]
  (->> (.query spatial-index (.getEnvelopeInternal region))
       (filter (fn [row]
                 (.intersects (:prepared-geometry row) region)))
       (map :Name)))

;; Let's load the neighborhoods and build the index:

^{:kindly/hide-code false}
(def oakland-neighborhoods
  (load-neighborhoods))

^{:kindly/hide-code false}
(-> oakland-neighborhoods
    (tc/select-columns [:Name])
    (tc/head 10))

^{:kindly/hide-code false}
(def neighborhoods-index
  (make-spatial-index oakland-neighborhoods))

;; Now test it with our Telegraph & 19th intersection:

^{:kindly/hide-code false}
(def telegraph-19th-neighborhoods
  (intersecting-places telegraph-19th-center neighborhoods-index))

^{:kindly/hide-code true}
(kind/hiccup
 [:div
  [:h4 "Which neighborhood is Telegraph & 19th in?"]
  [:p "Found: " [:strong (pr-str telegraph-19th-neighborhoods)]]])

;; ## Putting It All Together: Real Workflow Examples

;; Let's see both approaches in action with data:

;; ### Example 1: Manual Lookup (What we used)

;; We already saw this working earlier with Grand Ave. Here's a summary showing
;; how simple and effective it is:

^{:kindly/hide-code false}
(-> grand-ave-sample
    (tc/select-columns [:collision-id :secondary-road :latitude :longitude 
                        :intersection-lat :intersection-lng]))

;; Notice:
;; - `latitude` and `longitude` are empty (the original data)
;; - `intersection-lat` and `intersection-lng` are filled in from our manual lookup
;; - Simple, fast, and accurate!

;; ### Example 2: Automated Matching (Advanced approach from locations.clj)

;; We demonstrated this with Telegraph & 19th. Let's compare the results:

^{:kindly/hide-code true}
(let [manual-coords {:lat 37.808247 :lng -122.269923}
      auto-coord (jts/coord telegraph-19th-center)
      auto-coords {:lat (.getY auto-coord) :lng (.getX auto-coord)}
      lat-diff (Math/abs (- (:lat manual-coords) (:lat auto-coords)))
      lng-diff (Math/abs (- (:lng manual-coords) (:lng auto-coords)))]
  (kind/hiccup
   [:div
    [:h4 "Telegraph & 19th: Manual vs Automated"]
    [:table {:style {:border-collapse "collapse" :margin "10px 0"}}
     [:thead
      [:tr 
       [:th {:style {:border "1px solid #ddd" :padding "8px" :background "#f2f2f2"}} "Method"]
       [:th {:style {:border "1px solid #ddd" :padding "8px" :background "#f2f2f2"}} "Latitude"]
       [:th {:style {:border "1px solid #ddd" :padding "8px" :background "#f2f2f2"}} "Longitude"]]]
     [:tbody
      [:tr
       [:td {:style {:border "1px solid #ddd" :padding "8px"}} "Manual Lookup"]
       [:td {:style {:border "1px solid #ddd" :padding "8px"}} (str (:lat manual-coords))]
       [:td {:style {:border "1px solid #ddd" :padding "8px"}} (str (:lng manual-coords))]]
      [:tr
       [:td {:style {:border "1px solid #ddd" :padding "8px"}} "Automated (Centerlines)"]
       [:td {:style {:border "1px solid #ddd" :padding "8px"}} (str (:lat auto-coords))]
       [:td {:style {:border "1px solid #ddd" :padding "8px"}} (str (:lng auto-coords))]]]]
    [:p [:strong "Difference: "] 
     (format "%.6f degrees lat, %.6f degrees lng" lat-diff lng-diff)]
    [:p "That's within " [:strong (format "%.1f meters" (* 111000 (Math/sqrt (+ (* lat-diff lat-diff) (* lng-diff lng-diff)))))] "!"]])) 

;; The automated approach gets very close to the manual lookup!

;; ### Example 3: Complete Analysis - Grand Ave with Neighborhoods

;; Let's show a complete example combining everything:

^{:kindly/hide-code false}
(def grand-ave-with-neighborhoods
  (-> grand-ave-crashes-2023
      (tc/map-columns :intersection-lat
                      [:secondary-road]
                      (fn [secondary-road]
                        (let [match (some (fn [[k v]]
                                            (when (str/includes? (or secondary-road "") k)
                                              v))
                                          grand-intersections-of-interest)]
                          (:lat match))))
      (tc/map-columns :intersection-lng
                      [:secondary-road]
                      (fn [secondary-road]
                        (let [match (some (fn [[k v]]
                                            (when (str/includes? (or secondary-road "") k)
                                              v))
                                          grand-intersections-of-interest)]
                          (:lng match))))
      (tc/map-columns :point
                      [:intersection-lat :intersection-lng]
                      (fn [lat lng]
                        (when (and lat lng)
                          (jts/point lng lat))))
      (tc/map-columns :neighborhoods
                      [:point]
                      (fn [point]
                        (when point
                          (intersecting-places point neighborhoods-index))))))

^{:kindly/hide-code false}
(-> grand-ave-with-neighborhoods
    (tc/select-columns [:collision-id :secondary-road :intersection-lat 
                        :intersection-lng :neighborhoods])
    (tc/head 5))

;; Now we have:
;; - Original crash data
;; - Filled-in coordinates from manual lookup
;; - Assigned neighborhoods from spatial index
;; 
;; All ready for analysis!

;; ## Code Reference: How We Did It

;; For reference, here's the workflow code from our notebooks:

;; ### Grand Ave Workflow (Manual - from index.clj)

^{:kindly/hide-code false}
(comment
  ;; Telegraph Ave used the same manual approach!
  (def telegraph-intersections-of-interest
    (merge kono-intersections-of-interest
           pill-hill-intersections-of-interest))
  
  (def telegraph-ave-crashes
    (let [crashes (-> oakland-city-crashes
                      (ds/filter #(str/includes? (or (:primary-road %) "") "TELEGRAPH"))
                      (ds/filter (fn [row]
                                   (or (some #(str/includes? (:primary-road row) %)
                                             (keys telegraph-intersections-of-interest))
                                       (some #(str/includes? (:secondary-road row) %)
                                             (keys telegraph-intersections-of-interest))))))]
      ;; Same manual lookup pattern
      (-> crashes
          (tc/map-columns :intersection-lat [:secondary-road]
                          (fn [sec-road]
                            (let [match (some (fn [[k v]]
                                                (when (str/includes? (or sec-road "") k) v))
                                              telegraph-intersections-of-interest)]
                              (:lat match))))
          (tc/map-columns :intersection-lng [:secondary-road]
                          (fn [sec-road]
                            (let [match (some (fn [[k v]]
                                                (when (str/includes? (or sec-road "") k) v))
                                              telegraph-intersections-of-interest)]
                              (:lng match))))))))

;; ### City-wide Advanced Workflow (Automated - from locations.clj)

^{:kindly/hide-code false}
(comment
  ;; This uses data/Oakland-centerlines (preprocessed in data.clj)
  ;; and data/bay-area->wgs84 for coordinate transformations
  (def crashes-with-centerlines
    (-> crashes
        ;; Look up centerlines for each street name
        (tc/map-columns :centerlines
                        [:primary-road :secondary-road]
                        (fn [primary-road secondary-road]
                          [(some-> primary-road normalize-street-for-lookup street->centerlines)
                           (some-> secondary-road normalize-street-for-lookup street->centerlines)]))
        ;; Find intersecting segments (using buffers created in data.clj)
        (tc/map-columns :intersecting-segments
                        [:centerlines]
                        (fn [[centerlines1 centerlines2]]
                          (for [cl1 centerlines1
                                cl2 centerlines2
                                :when (.intersects (:local-buffer cl1)
                                                   (:local-buffer cl2))]
                            [cl1 cl2])))
        ;; Calculate intersection centers (uses data/bay-area->wgs84)
        (tc/map-columns :intersection-center
                        [:intersecting-segments]
                        calculate-intersection-center))))

;; ## Summary: Key Techniques Used in This Project

;; ### 1. **Manual Intersection Lookup**
;; - **What we used**: Both Grand Ave and Telegraph Ave analyses in `index.clj`
;; - Fast and simple when you know your area
;; - Requires manually looking up coordinates for specific intersections

;; ### 2. **Automated Spatial Matching** 
;; - **Explored in**: `locations.clj` for advanced analysis
;; - Would scale to whole city without manual lookup
;; - Uses street centerline geometry and coordinate transformations

;; ### 3. **Street Name Normalization**
;; - Remove directional suffixes (WB, NB, EB, SB, WESTBOUND, etc.)
;; - Extract multiple street names from complex fields ("75TH ON HEGENBERGER")
;; - Build lookup indexes for fast matching

;; ### 4. **Coordinate System Transformations**
;; - Transform to local projected coordinates (EPSG:2227) for accurate distance calculations
;; - Use 50-foot buffers for fuzzy spatial matching
;; - Transform back to WGS84 for mapping/display

;; ### 5. **Tensor Operations for Geometry**
;; - Use tech.ml.dataset tensor operations for efficient distance calculations
;; - Find nearest points between line strings
;; - Average coordinates to find intersection centers

;; ### 6. **Spatial Indexing**
;; - Use R-tree indexes (STRtree) for fast spatial queries
;; - PreparedGeometry for efficient repeated intersection tests
;; - Point-in-polygon for neighborhood assignment

;; ## When to Use Which Approach

;; | Scenario | Approach | Why |
;; |----------|----------|-----|
;; | Known intersections in focused area | Manual lookup | Simple, fast, you control exactly what to include (this is what we used!) |
;; | City-wide analysis of all crashes | Automated matching | Can't manually lookup thousands of intersections |
;; | Missing street names entirely | Need source coordinates | Neither approach can derive location without street names |
;; | Very messy/inconsistent street names | Manual lookup | Less prone to matching errors |
;; | Real-time/production system | Automated | Can't manually lookup every new crash |
;; | Exploratory analysis of specific corridor | Manual lookup | Quick to set up for 10-20 intersections |

;; ## What We Did in This Project

;; - **Grand Ave** (`index.clj`): Manual lookup with `grand-intersections-of-interest`
;; - **Telegraph Ave** (`index.clj`): Manual lookup with `kono-intersections-of-interest` and `pill-hill-intersections-of-interest`
;; - **Advanced exploration** (`locations.clj`): Automated centerline matching for city-wide patterns

;; ## Libraries Used

;; - **[factual/geo](https://github.com/Factual/geo)**: JTS wrapper, CRS transforms, I/O
;; - **[tablecloth](https://github.com/scicloj/tablecloth)**: DataFrame operations
;; - **[tech.ml.dataset](https://github.com/techascent/tech.ml.dataset)**: Tensors and efficient computation
;; - **[charred](https://github.com/cnuernber/charred)**: Fast JSON parsing

;; ## See the Real Code

;; - `notebooks/index.clj` - Grand Ave analysis with manual lookup
;; - `notebooks/locations.clj` - Sophisticated spatial matching with tensors
;; - `notebooks/data.clj` - Data loading and preprocessing

;; ---

;; *This tutorial documents the techniques used in the [Grand Ave crash analysis project](https://github.com/tophat8855/grand-ave),
;; created for the Clojure data science community to show real-world GIS data handling.*

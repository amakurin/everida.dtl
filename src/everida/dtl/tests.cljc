(ns everida.dtl.tests
  (:require
    [everida.dtl.core :as core]
    [everida.dtl.transformers.comparison :as comp]
    [everida.dtl.transformers.mapping :as map]
    [everida.dtl.transformers.str :as str]
    #?(:clj  [clojure.test :refer [deftest is testing run-tests use-fixtures]]
       :cljs [cljs.test :refer-macros [deftest is testing run-tests use-fixtures]])
    ))


;; ============================== MAPPING begin
(def map-test-data
  {:params       {:hub-id 23}
   :query-params {:limit 1 :offset 2 :search-string 3}})

(deftest map-to-map-simple
  (let [rule [:dtlrules {:open {:qn :app/browse-hub}}
              [:filter [:dtlrules
                        [:db/id [[:params :hub-id]]]
                        [:search-string [[:query-params :search-string]]]]]
              [:limit [[:query-params :limit]]]
              [:offset [[:query-params :offset]]]]]
    (is (= (core/transform map-test-data rule map/transformer)
           {:qn :app/browse-hub, :filter {:db/id 23, :search-string 3}, :limit 1, :offset 2}))))

(deftest map-to-map-simple-1
  (let [rule [:dtlrules {:open {:qn :app/browse-hub}}
              [[:filter :db/id] [[:params :hub-id]]]
              [:limit [[:query-params :limit]]]
              [:offset [[:query-params :offset]]]
              [:search-string [[:query-params :search-string]]]]]
    (is (= (core/transform map-test-data rule map/transformer)
           {:qn :app/browse-hub, :filter {:db/id 23}, :limit 1, :offset 2, :search-string 3}))))


(deftest map-to-vec
  (let [rule [:dtlrules {:open [] :& :concat-vectors}
              [[:filter :db/id] [[:params :hub-id]]]
              [:limit [[:query-params :limit]]]
              [:offset [[:query-params :offset]]]
              [:search-string [[:query-params :search-string]]]]]
    (is (= (core/transform map-test-data rule map/transformer)
           [{:filter {:db/id 23}} {:limit 1} {:offset 2} {:search-string 3}]))))

(deftest map-to-map-deep-merge
  (let [rule [:dtlrules {:open {:qn :app/browse-hub} :& :deep-merge-map}
              [[:filter :db/id] [[:params :hub-id]]]
              [:limit [[:query-params :limit]]]
              [:offset [[:query-params :offset]]]
              [[:filter :search-string] [[:query-params :search-string]]]]]
    (is (= (core/transform map-test-data rule map/transformer)
           {:qn :app/browse-hub, :filter {:db/id 23, :search-string 3}, :limit 1, :offset 2}))))

(deftest map-to-map-mapcat-values
  (let [rule [:dtlrules {:open {:qn :app/browse-hub} :& :deep-merge-map}
              [[:filter :db/id] [[:params :hub-id]]]
              [:limit [[:query-params :limit]]]
              [:offset [[:query-params :offset]]]
              [[:filter :search-string] [[:query-params :search-string]]]]]
    (is (= (core/transform map-test-data rule map/transformer)
           {:qn :app/browse-hub, :filter {:db/id 23, :search-string 3}, :limit 1, :offset 2}))))

#_(time
    (let [rule [:dtlrules {:open {:qn :app/browse-hub} :& :deep-merge-map}
                [[:filter :db/id] [[:params :hub-id]]]
                [:limit [[:query-params :limit]]]
                [:offset [[:query-params :offset]]]
                [[:filter :search-string] [[:query-params :search-string]]]]]
      (core/transform map-test-data rule map/transformer)))

;; ============================== MAPPING end

;; ============================== COMPARISON begin

(def comp-test-data
  {:qn            :app/browse-hub,
   :filter        {:db/id 23},
   :limit         25,
   :offset        50,
   :search-string "Hub name"})

(deftest matched-simple
  (let [rule [23 [[:filter :db/id]]]]                       ;; only one rule = simplified form
    (is (core/transform comp-test-data rule comp/transformer))))

(deftest matched-simple-2
  (let [rule [:app/browse-hub [:qn]]]                       ;; only one rule + one key in path
    (is (core/transform comp-test-data rule comp/transformer))))

(deftest non-matched-simple
  (let [rule [0 [[:filter :db/id]]]]                        ;; only one rule = simplified form
    (is (not (core/transform comp-test-data rule comp/transformer)))))

(deftest matched-or
  (let [rule [:dtlrules {:& :or}                            ;; join following comparisons with logic OR
              [23 [[:filter :db/id]]]                       ;; is 23 = data on path [:filter :db/id]?
              [25 [:limit]]]                                ;; is 25 = data on path [:limit]?
        ]
    (is (core/transform comp-test-data rule comp/transformer))))

(deftest matched-or-1
  (let [rule [:dtlrules {:& :or}
              [23 [[:filter :db/id]]]
              [0 [:limit]]]
        ]
    (is (core/transform comp-test-data rule comp/transformer))))

(deftest non-matched-or
  (let [rule [:dtlrules {:& :or}
              [0 [[:filter :db/id]]]
              [1 [:limit]]]
        ]
    (is (not (core/transform comp-test-data rule comp/transformer)))))


(deftest matched-and
  (let [rule [:dtlrules {:& :and}                           ;; join following comparisons with logic AND
              [23 [[:filter :db/id]]]
              [25 [:limit]]]
        ]
    (is (core/transform comp-test-data rule comp/transformer))))

(deftest non-matched-and
  (let [rule [:dtlrules {:& :and}
              [0 [[:filter :db/id]]]
              [25 [:limit]]]
        ]
    (is (not (core/transform comp-test-data rule comp/transformer)))))

#_(time
    (let [rule [:dtlrules {:& :and}
                [23 [[:filter :db/id]]]
                [25 [:limit]]]]
      (is (core/transform comp-test-data rule comp/transformer))))

;; ============================== COMPARISON end

;; ============================== STR begin

(deftest str-simple
  (let [data {:qn            :app/browse-hub,
              :filter        {:db/id 23},
              :limit         25,
              :offset        50,
              :search-string "Hub name"}
        rule [:dtlrules
              ["/hubs/%s/" [[:filter :db/id]]]
              ["%s" [:dtlrules {:? "?" :& "&"}
                     ["limit=%s" [:limit]]
                     ["offset=%s" [:offset]]
                     ["search-string=%s" [:search-string]]]]]]
    (is (= (core/transform data rule str/transformer)
           "/hubs/23/?limit=25&offset=50&search-string=Hub name"))))

(deftest str-simple-fn
  (let [data {:day 1 :month-k :jan :year 2020 :hour 4}
        rule ["%s %s %sy at %s:%02d" [:day :month-k :year :hour {:p :minute :or 0}]]]
    (is (= (core/transform data rule str/transformer
                           (fn [_ x] (if (= x :jan) "jan" x)))
           "1 jan 2020y at 4:00"))))

#_(time
    (core/transform
      str-test-data
      [:dtlrules
       ["/hubs/%s/" [[:filter :db/id]]]
       ["%s" [:dtlrules {:? "?" :& "&"}
              ["limit=%s" [:limit]]
              ["offset=%s" [:offset]]
              ["search-string=%s" [:search-string]]]]]
      str/transformer))

;; ============================== STR end


#_(require '[everida.dtl.tests])
#_(run-tests)
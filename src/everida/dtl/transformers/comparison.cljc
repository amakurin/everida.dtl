(ns everida.dtl.transformers.comparison
  (:require [everida.utils :as utils]))

(defmulti compose-dtlrules
          (fn [_ dtlrules-options] (:& dtlrules-options)))

(defmethod compose-dtlrules :or
  [dtlrules-results _]
  (some identity dtlrules-results))

(defmethod compose-dtlrules :and
  [dtlrules-results _]
  (every? identity dtlrules-results))

(defn compare-set [compare-set v]
  (if (nil? v)
    (contains? compare-set nil)
    (compare-set v)))

(defn transformer
  "Transforms map to bool according transformation rule (see dtl.core grammar)
  Transformer specific notes:
  * composition-rule = value or set of values to compare over usings, nonset value will-be wrapped in set
  * open-element, prefix-element, close-element = ignored
  * join-element = ':or' | ':and'
  - :or applies 'logic or' to all dtlrules comparisons
  - :and applies 'logic and' to all dtlrules comparisons
  * Miltiple usings are reduced with 'logic and'
  * If using value is a set, then not empty intersection with composition-rule means true."
  [transformation-context]
  (let [{:keys [rule use-data root? children dtlrules-options]} transformation-context]
    (if children
      (let [[composition-rule] rule
            dtlrules-options (update dtlrules-options :& utils/alternate-value :and)
            result (compose-dtlrules children dtlrules-options)]
        (if root? result ((utils/ensure-set composition-rule) result)))
      (let [[composition-rule _] rule
            comparator (utils/ensure-set composition-rule)]
        (if (empty? use-data)
          composition-rule
          (every? (fn [using]
                    (if (set? using)
                      (some (partial compare-set comparator) using)
                      (compare-set comparator using)))
                  use-data))))))
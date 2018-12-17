(ns everida.dtl.transformers.mapping
  (:require [everida.utils :as utils]))

(defmulti compose-dtlrules
          (fn [_ dtlrules-options] (:& dtlrules-options)))

(defmethod compose-dtlrules :merge-map
  [dtlrules-results {:keys [open ? close]}]
  (merge (apply merge open ? dtlrules-results) close))

(defmethod compose-dtlrules :deep-merge-map
  [dtlrules-results {:keys [open ? close] :or {open {} ? {} close {}}}]
  (utils/deep-merge (apply utils/deep-merge open ? dtlrules-results) close))

(defmethod compose-dtlrules :concat-vectors
  [dtlrules-results {:keys [open ? close]}]
  (vec (concat open (when ? [?]) dtlrules-results close)))

(defmethod compose-dtlrules :mapcat-values
  [dtlrules-results {:keys [open ? close]}]
  (vec (mapcat vals (concat open (when ? [?]) dtlrules-results close))))

(defn transformer
  "Transforms map to map according transformation rule (see dtl.core grammar)
  Transformer notes:
  * composition-rule = [keyword+] path to value of resulting map (simple keywords are allowed, will be wrapped to vector)
  * open-element, prefix-element, close-element = map or any val acceptable by operation of join-element or using-data-preprocessor function (default {})
  * join-element = ':merge-map' | ':concat-vectors' | ':deep-merge-map' | ':mapcat-values'
  - :merge-map is default and merges all results into open-element-map
  - :deep-merge-map deeply merges all results into open-element-map or {}
  - :concat-vectors concats all results and vec result
  - :mapcat-values mapcat vals all over results and vec result
  * Usings associates as vector, single using as using value."
  [transformation-context]
  (let [{:keys [rule use-data root? children dtlrules-options]} transformation-context]
    (if children
      (let [[composition-rule] rule
            dtlrules-options (->
                              dtlrules-options
                              (update :open utils/alternate-value {})
                              (update :& utils/alternate-value :merge-map))
            composition-rule (utils/ensure-vector composition-rule)
            result (compose-dtlrules
                     (keep identity children) dtlrules-options)]
        (if root? result (assoc-in {} composition-rule result)))
      (let [[composition-rule _] rule
            composition-rule (utils/ensure-vector composition-rule)
            result (if (seq (rest use-data)) use-data (first use-data))]
        (when result
          (assoc-in {} composition-rule result))))))

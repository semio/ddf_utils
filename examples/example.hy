;; This is an example recipe in Hy language
;; To run the recipe, simply call `hy example.hy` in terminal
;;
;; This example we create a dataset with 3 population indicators
;; population_0_4, population_5_9, population_10_19 (by geo, year)
;; each one repercented the population for an age group.
;; The source is population by age dataset.

(require [ddf_utils.chef.hy_mod.macros [*]])
(import [ddf_utils.chef.helpers [gen_sym]])
(import [datetime [datetime]])

(init)

(info :id "population-age-group-dataset"
      :last_update (str (datetime.today)))

(config :ddf_dir "path/to/dataset/dir")

(ingredient :id "population-by-age-datapoints"
            :dataset "ddf--unpop--wpp_population"
            :key "country_code,year,age"
            :value ["population"])

;; now we create procedures to generate the indicators.
;; all three indicators should calculated with the same procedures:
;; - filter the population by age indicator, get only data for target age group
;; - group filtered data by country_code, year, and the the aggregated sum
;; - because we want the indicators' primaryKeys to be geo, year, we need to do translation.
;;
;; Because procedures are the same, we use loop to add procedures dynamically.

(setv collection "datapoints"
      ;; 3 groups, (range a b) gives a range of integers, and (str x) gives string form of x
      ;; list-comp is like map()
      groups [(list-comp (str x) [x (range 0 5)])
              (list-comp (str x) [x (range 5 10)])
              (list-comp (str x) [x (range 10 20)])]
      ;; names for each group
      names ["population_0_4"
             "population_5_9"
             "population_10_19"]
      ingredient_source "population-by-age-datapoints")

(defn make_age_group_dps [ingredient age_group indicator_name]
  ;; create datapoints ingredient from age_group and indicator_name
  (setv filtered_result (+ "filtered-" indicator_name))
  (setv groupby_result (+ "sum-" filtered_result))
  (setv translated_result (+ "translated-" groupby_result))

  (procedure filtered_result collection
             :procedure "filter"
             :ingredients [ingredient]
             :options {:row {
                             :age {"$in" age_group}}})
  (procedure groupby_result collection
             :procedure "groupby"
             :ingredients [ingredient]
             :options {:groupby ["country_code" "year"]
                       :aggregate {:population "sum"}})
  (procedure translated_result collection
             :procedure "translate_header"
             :ingredients [ingredient]
             :options {:dictionary {"country_code" "geo"}})
  ;; finally return the last result id
  translated_result)

;; now add procedures for each group
(setv results [])
(for [(, g n) (zip groups names)]
  (setv res (make_age_group_dps ingredient_source g n))
  (results.append res))

;; merge them
(procedure "merged-result" "datapoints"
           :procedure "merge"
           :ingredients results)

;; serve them
(serve :ingredients ["merged-result"])

;; print the recipe
;; or (run) to run the recipe
(print (*chef*.to_recipe))


;; -*- mode: clojure;-*-

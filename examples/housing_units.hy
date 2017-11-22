;;

(setv url "https://github.com/Gapminder/ddf--pcbs--census")

(import [ddf_utils.chef.api [Chef]])
(import [ddf_utils.chef.hy_mod.funcs :as f])
(require [ddf_utils.chef.hy_mod.macros [*]])

(init)

(info :name "test_dataset"
      :author "Semio Z"
      :date "2017121")

(config :ddf_dir "/tmp"
        :recipes_dir "./"
        :procedure_dir "./")

(ingredient :id "datapoints-housing_units"
            :dataset url
            :key "geo, year"
            :value ["housing_units"])

(ingredient :id "datapoints-connectioin_to_electricity"
            :dataset url
            :key "geo, year"
            :value ["connection_to_electricity"])

(ingredient :id "datapoints-housing_units-by-tenure"
            :dataset url
            :key "tenure, geo, year"
            :value ["housing_units"])

(def cooking "datapoints")

(procedure "dps-housing_units-flatten" cooking
            :procedure "flatten"
            :ingredients "datapoints-housing_units-by-tenure"
            :options {:flatten_dimensions ["tenure"]
                      :dictionary
                      {"housing_units" "housing_units_{tenure}"}})

(procedure "dps-housing_unit_merged" cooking
            :procedure "merge"
            :ingredients ["dps-housing_units-flatten"
                          "datapoints-housing_units"
                          "datapoints-connection_to_electricity"])

(procedure "dps-housing_uint_rates" cooking
            :procedure "run_op"
            :ingredients ["dps-housing_uint_merged"]
            :options {:op {:housing_units_owned_percentage
                          "housing_units_owned / housing_units * 100"
                          :housing_units_rented_un_furnished_percentage
                          "housing_units_rented_un_furnished / housing_units * 100"
                          :housing_units_rented_furnished_percentage
                          "housing_units_rented_furnished / housing_units * 100"
                          :housing_units_without_payment_percentage
                          "housing_units_without_payment / housing_units * 100"
                          :housing_units_for_work_percentage
                          "housing_units_for_work / housing_units * 100"
                          :housing_units_others_percentage
                          "housing_units_others / housing_units * 100"
                          :housing_units_not_stated_percentage
                          "housing_units_not_stated / housing_units * 100"
                          :connection_to_electricity_percentage
                          "connection_to_electricity / housing_units * 100"}})
(serve cooking
       :ingredients ["dps-housing_unit_rates"
                     "dps-housing_units-flatten"
                     "datapoints-housing_units"])

(print (*chef*.to_recipe))

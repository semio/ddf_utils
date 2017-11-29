
(defmacro init []
  `(do
     (import [ddf_utils.chef.api [Chef]])
     (import [ddf_utils.chef.hy_mod.funcs :as _f])
     (setv *chef* (Chef))))

(defmacro info [&rest code]
  `(_f.info *chef* ~@code))

(defmacro config [&rest code]
  `(_f.config *chef* ~@code))

(defmacro ingredient [&rest code]
  `(_f.ingredient *chef* ~@code))

(defmacro procedure [&rest code]
  `(_f.procedure *chef* ~@code))

(defmacro serve [&rest code]
  `(_f.serve *chef* ~@code))

(defmacro run [&rest code]
  `(setv res (*chef*.run ~@code)))

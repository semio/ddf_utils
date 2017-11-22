
(defmacro init []
  `(setv *chef* (Chef)))

(defmacro info [&rest code]
  `(f.info *chef* ~@code))

(defmacro config [&rest code]
  `(f.config *chef* ~@code))

(defmacro ingredient [&rest code]
  `(f.ingredient *chef* ~@code))

(defmacro procedure [&rest code]
  `(f.procedure *chef* ~@code))

(defmacro serve [&rest code]
  `(f.serve *chef* ~@code))

;; (defmacro info [&kwargs kwargs]
;;   `(apply (. *chef* add_metadata) [] ~kwargs))

;; (defmacro config [&kwargs kwargs]
;;   (apply (. *chef* add_config) [] kwargs))

;; (defmacro show [&optional how]
;;   (cond [(= how "recipe") (.to_recipe *chef*)]
;;         [True (print (. *chef* metadata))]))

;; (defmacro procedure [result collection &kwargs kwargs]
;;   (do
;;     (setv (. kwargs ["collection"]) collection)
;;     (setv (. kwargs ["result"]) result)
;;     (apply (. *chef* add_procedure) [] kwargs)))

;; (defmacro ingredient [&kwargs kwargs]
;;   (apply (. *chef* add_ingredient) [] kwargs))

;; (defmacro serve [collection &kwargs kwargs]
;;   (setv (. kwargs ["collection"]) collection)
;;   (setv (. kwargs ["procedure"]) "serve")
;;   (apply (. *chef* add_procedure) [] kwargs))


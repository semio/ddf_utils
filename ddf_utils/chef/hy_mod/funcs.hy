(import [ddf_utils.chef.api [Chef]])

;; (defn init []
;;   (global *chef*)
;;   (setv *chef* (Chef)))

(defn info [chef &kwargs kwargs]
  (apply (. chef add_metadata) [] kwargs))

(defn config [chef &kwargs kwargs]
  (apply (. chef add_config) [] kwargs))

;; (defn ingredients [ingreds]
;;   (for [i ingreds] (do
;;     (print i)
;;     (apply (. *chef* add_ingredient) [] i))))

;; (defn show [&optional how]
;;   (cond [(= how "recipe") (.to_recipe *chef*)]
;;         [True (print (. *chef* metadata))]))

(defn procedure [chef result collection &kwargs kwargs]
    (do
     (setv (. kwargs ["collection"]) collection)
     (setv (. kwargs ["result"]) result)
     ; (pprint.pprint kwargs)))
     (apply (. chef add_procedure) [] kwargs)))

(defn ingredient [chef &kwargs kwargs]
  (apply (. chef add_ingredient) [] kwargs))

(defn serve [chef collection &kwargs kwargs]
  (setv (. kwargs ["collection"]) collection)
  (setv (. kwargs ["procedure"]) "serve")
  (apply (. chef add_procedure) [] kwargs))

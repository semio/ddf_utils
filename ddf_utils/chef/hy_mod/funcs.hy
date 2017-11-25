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
     (setv kwargs (convert_keyword kwargs))
     ; (pprint.pprint kwargs)))
     (apply (. chef add_procedure) [] kwargs)))

(defn get_name [k]
  ;;; convert keyword to string. because the default `name` function will
  ;;; replace underscroce, we create a new function here.
  (.replace (name k) "-" "_"))

(defn convert_keyword [d]
  (setv new_dict (dict))
  (for [(, k v) (.items d)]
    (if (instance? dict v)
        (setv v_new (convert_keyword v))
        (setv v_new v))
    (if (keyword? k)
        (setv (. new_dict [(get_name k)]) v_new)
        (setv (. new_dict [k]) v_new)))
  new_dict)

(defn ingredient [chef &kwargs kwargs]
  (apply (. chef add_ingredient) [] kwargs))

(defn serve [chef &kwargs kwargs]
  (setv (. kwargs ["collection"]) "")
  (setv (. kwargs ["procedure"]) "serve")
  (apply (. chef add_procedure) [] kwargs))


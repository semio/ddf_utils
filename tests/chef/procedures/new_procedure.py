from ddf_utils.chef.model.ingredient import DataPointIngredient


def multiply_1000(chef, ingredients, result, **options):
    # ingredients = [chef.dag.get_node(x) for x in ingredients]
    ingredient = ingredients[0]

    new_data = dict()
    for k, df in ingredient.get_data().items():
        df_ = df.copy()
        df_[k] = df_[k] * 1000
        new_data[k] = df_

    return DataPointIngredient.from_procedure_result(result, ingredient.key, new_data)

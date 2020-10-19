from ddf_utils.chef.model.ingredient import DataPointIngredient

def proc(chef, ingredients, result, **options):
	ingredient = ingredients[0]

	new_data = dict()
	for k, df in ingredient.get_data().items():
	    df_ = df.copy()
	    df_[k] = df_[k].map(lambda x: 0 if x < 0 else x)
	    new_data[k] = df_

	return DataPointIngredient.from_procedure_result(result, ingredient.key, new_data)

Search.setIndex({docnames:["ddf_utils","ddf_utils.chef","ddf_utils.chef.model","ddf_utils.chef.procedure","ddf_utils.factory","ddf_utils.model","factory","index","intro","modules","recipe","utils"],envversion:{"sphinx.domains.c":1,"sphinx.domains.changeset":1,"sphinx.domains.citation":1,"sphinx.domains.cpp":1,"sphinx.domains.javascript":1,"sphinx.domains.math":2,"sphinx.domains.python":1,"sphinx.domains.rst":1,"sphinx.domains.std":1,"sphinx.ext.intersphinx":1,sphinx:56},filenames:["ddf_utils.rst","ddf_utils.chef.rst","ddf_utils.chef.model.rst","ddf_utils.chef.procedure.rst","ddf_utils.factory.rst","ddf_utils.model.rst","factory.rst","index.rst","intro.rst","modules.rst","recipe.rst","utils.rst"],objects:{"ddf_utils.chef":{api:[1,0,0,"-"],exceptions:[1,0,0,"-"],helpers:[1,0,0,"-"],ops:[1,0,0,"-"]},"ddf_utils.chef.api":{run_recipe:[1,1,1,""]},"ddf_utils.chef.exceptions":{ChefRuntimeError:[1,2,1,""],IngredientError:[1,2,1,""],ProcedureError:[1,2,1,""]},"ddf_utils.chef.helpers":{build_dictionary:[1,1,1,""],build_dictionary_from_dataframe:[1,1,1,""],build_dictionary_from_file:[1,1,1,""],create_dsk:[1,1,1,""],debuggable:[1,1,1,""],dsk_to_pandas:[1,1,1,""],gen_query:[1,1,1,""],gen_sym:[1,1,1,""],get_procedure:[1,1,1,""],mkfunc:[1,1,1,""],prompt_select:[1,1,1,""],query:[1,1,1,""],read_opt:[1,1,1,""],sort_df:[1,1,1,""]},"ddf_utils.chef.model":{chef:[2,0,0,"-"],dag:[2,0,0,"-"],ingredient:[2,0,0,"-"]},"ddf_utils.chef.model.chef":{Chef:[2,3,1,""]},"ddf_utils.chef.model.chef.Chef":{add_config:[2,4,1,""],add_dish:[2,4,1,""],add_ingredient:[2,4,1,""],add_metadata:[2,4,1,""],add_procedure:[2,4,1,""],config:[2,4,1,""],copy:[2,4,1,""],from_recipe:[2,4,1,""],ingredients:[2,4,1,""],register_procedure:[2,4,1,""],run:[2,4,1,""],serving:[2,4,1,""],to_graph:[2,4,1,""],to_recipe:[2,4,1,""],validate:[2,4,1,""]},"ddf_utils.chef.model.dag":{BaseNode:[2,3,1,""],DAG:[2,3,1,""],IngredientNode:[2,3,1,""],ProcedureNode:[2,3,1,""]},"ddf_utils.chef.model.dag.BaseNode":{add_downstream:[2,4,1,""],add_upstream:[2,4,1,""],detect_missing_dependency:[2,4,1,""],downstream_list:[2,4,1,""],evaluate:[2,4,1,""],get_direct_relatives:[2,4,1,""],upstream_list:[2,4,1,""]},"ddf_utils.chef.model.dag.DAG":{add_dependency:[2,4,1,""],add_node:[2,4,1,""],copy:[2,4,1,""],detect_cycles:[2,4,1,""],get_node:[2,4,1,""],has_node:[2,4,1,""],node_dict:[2,4,1,""],nodes:[2,4,1,""],roots:[2,4,1,""],tree_view:[2,4,1,""]},"ddf_utils.chef.model.dag.IngredientNode":{evaluate:[2,4,1,""]},"ddf_utils.chef.model.dag.ProcedureNode":{evaluate:[2,4,1,""]},"ddf_utils.chef.model.ingredient":{ConceptIngredient:[2,3,1,""],DataPointIngredient:[2,3,1,""],EntityIngredient:[2,3,1,""],Ingredient:[2,3,1,""],get_ingredient_class:[2,1,1,""],ingredient_from_dict:[2,1,1,""],key_to_list:[2,1,1,""]},"ddf_utils.chef.model.ingredient.ConceptIngredient":{dtype:[2,5,1,""],get_data:[2,4,1,""],get_data_from_ddf_dataset:[2,4,1,""],get_data_from_external_csv:[2,4,1,""],get_data_from_inline_data:[2,4,1,""],serve:[2,4,1,""]},"ddf_utils.chef.model.ingredient.DataPointIngredient":{compute:[2,4,1,""],dtype:[2,5,1,""],from_procedure_result:[2,4,1,""],get_data:[2,4,1,""],get_data_from_ddf_dataset:[2,4,1,""],get_data_from_external_csv:[2,4,1,""],get_data_from_inline_data:[2,4,1,""],serve:[2,4,1,""]},"ddf_utils.chef.model.ingredient.EntityIngredient":{dtype:[2,5,1,""],get_data:[2,4,1,""],get_data_from_ddf_dataset:[2,4,1,""],get_data_from_external_csv:[2,4,1,""],get_data_from_inline_data:[2,4,1,""],serve:[2,4,1,""]},"ddf_utils.chef.model.ingredient.Ingredient":{dataset_path:[2,4,1,""],ddf_id:[2,4,1,""],dtype:[2,5,1,""],filter_row:[2,4,1,""],from_procedure_result:[2,4,1,""],get_data:[2,4,1,""],ingredient_type:[2,4,1,""],serve:[2,4,1,""]},"ddf_utils.chef.ops":{aagr:[1,1,1,""],between:[1,1,1,""],gt:[1,1,1,""],lt:[1,1,1,""],zcore:[1,1,1,""]},"ddf_utils.chef.procedure":{extract_concepts:[3,0,0,"-"],filter:[3,0,0,"-"],flatten:[3,0,0,"-"],groupby:[3,0,0,"-"],merge:[3,0,0,"-"],merge_entity:[3,0,0,"-"],run_op:[3,0,0,"-"],split_entity:[3,0,0,"-"],translate_column:[3,0,0,"-"],translate_header:[3,0,0,"-"],trend_bridge:[3,0,0,"-"],window:[3,0,0,"-"]},"ddf_utils.chef.procedure.extract_concepts":{extract_concepts:[3,1,1,""]},"ddf_utils.chef.procedure.filter":{filter:[3,1,1,""]},"ddf_utils.chef.procedure.flatten":{flatten:[3,1,1,""]},"ddf_utils.chef.procedure.groupby":{groupby:[3,1,1,""]},"ddf_utils.chef.procedure.merge":{merge:[3,1,1,""]},"ddf_utils.chef.procedure.merge_entity":{merge_entity:[3,1,1,""]},"ddf_utils.chef.procedure.run_op":{run_op:[3,1,1,""]},"ddf_utils.chef.procedure.split_entity":{split_entity:[3,1,1,""]},"ddf_utils.chef.procedure.translate_column":{translate_column:[3,1,1,""]},"ddf_utils.chef.procedure.translate_header":{translate_header:[3,1,1,""]},"ddf_utils.chef.procedure.trend_bridge":{trend_bridge:[3,1,1,""]},"ddf_utils.chef.procedure.window":{window:[3,1,1,""]},"ddf_utils.factory":{clio_infra:[4,0,0,"-"],common:[4,0,0,"-"],ihme:[4,0,0,"-"],ilo:[4,0,0,"-"],oecd:[4,0,0,"-"],worldbank:[4,0,0,"-"]},"ddf_utils.factory.clio_infra":{ClioInfraLoader:[4,3,1,""]},"ddf_utils.factory.clio_infra.ClioInfraLoader":{bulk_download:[4,4,1,""],has_newer_source:[4,4,1,""],load_metadata:[4,4,1,""],url:[4,5,1,""]},"ddf_utils.factory.common":{DataFactory:[4,3,1,""],download:[4,1,1,""],requests_retry_session:[4,1,1,""],retry:[4,1,1,""]},"ddf_utils.factory.common.DataFactory":{bulk_download:[4,4,1,""],has_newer_source:[4,4,1,""],load_metadata:[4,4,1,""]},"ddf_utils.factory.ihme":{IHMELoader:[4,3,1,""]},"ddf_utils.factory.ihme.IHMELoader":{bulk_download:[4,4,1,""],download_links:[4,4,1,""],has_newer_source:[4,4,1,""],load_metadata:[4,4,1,""],url_data:[4,5,1,""],url_hir:[4,5,1,""],url_metadata:[4,5,1,""],url_task:[4,5,1,""],url_version:[4,5,1,""]},"ddf_utils.factory.ilo":{ILOLoader:[4,3,1,""]},"ddf_utils.factory.ilo.ILOLoader":{bulk_download:[4,4,1,""],download:[4,4,1,""],has_newer_source:[4,4,1,""],indicator_meta_url_tmpl:[4,5,1,""],load_metadata:[4,4,1,""],main_url:[4,5,1,""],other_meta_url_tmpl:[4,5,1,""]},"ddf_utils.factory.oecd":{OECDLoader:[4,3,1,""]},"ddf_utils.factory.oecd.OECDLoader":{bulk_download:[4,4,1,""],data_url_tmpl:[4,5,1,""],datastructure_url_tmpl:[4,5,1,""],has_newer_source:[4,4,1,""],load_metadata:[4,4,1,""],metadata_url:[4,5,1,""]},"ddf_utils.factory.worldbank":{WorldBankLoader:[4,3,1,""]},"ddf_utils.factory.worldbank.WorldBankLoader":{bulk_download:[4,4,1,""],has_newer_source:[4,4,1,""],load_metadata:[4,4,1,""],url:[4,5,1,""]},"ddf_utils.i18n":{merge_translations_csv:[0,1,1,""],merge_translations_json:[0,1,1,""],split_translations_csv:[0,1,1,""],split_translations_json:[0,1,1,""]},"ddf_utils.io":{cleanup:[0,1,1,""],csvs_to_ddf:[0,1,1,""],download_csv:[0,1,1,""],dump_json:[0,1,1,""],open_google_spreadsheet:[0,1,1,""],serve_concept:[0,1,1,""],serve_datapoint:[0,1,1,""],serve_entity:[0,1,1,""]},"ddf_utils.model":{"package":[5,0,0,"-"],ddf:[5,0,0,"-"],repo:[5,0,0,"-"],utils:[5,0,0,"-"]},"ddf_utils.model.ddf":{Concept:[5,3,1,""],DDF:[5,3,1,""],DaskDataPoint:[5,3,1,""],DataPoint:[5,3,1,""],Entity:[5,3,1,""],EntityDomain:[5,3,1,""],PandasDataPoint:[5,3,1,""],Synonym:[5,3,1,""]},"ddf_utils.model.ddf.Concept":{to_dict:[5,4,1,""]},"ddf_utils.model.ddf.DDF":{get_datapoints:[5,4,1,""],get_entities:[5,4,1,""],get_synonyms:[5,4,1,""],indicators:[5,4,1,""]},"ddf_utils.model.ddf.DaskDataPoint":{data:[5,4,1,""]},"ddf_utils.model.ddf.DataPoint":{data:[5,4,1,""]},"ddf_utils.model.ddf.Entity":{to_dict:[5,4,1,""]},"ddf_utils.model.ddf.EntityDomain":{add_entity:[5,4,1,""],entity_sets:[5,4,1,""],get_entity_set:[5,4,1,""],has_entity:[5,4,1,""],to_dict:[5,4,1,""]},"ddf_utils.model.ddf.PandasDataPoint":{data:[5,4,1,""]},"ddf_utils.model.ddf.Synonym":{to_dataframe:[5,4,1,""],to_dict:[5,4,1,""]},"ddf_utils.model.package":{DDFSchema:[5,3,1,""],DDFcsv:[5,3,1,""],DataPackage:[5,3,1,""],Resource:[5,3,1,""],TableSchema:[5,3,1,""]},"ddf_utils.model.package.DDFSchema":{from_dict:[5,4,1,""]},"ddf_utils.model.package.DDFcsv":{entity_domain_to_categorical:[5,4,1,""],entity_set_to_categorical:[5,4,1,""],from_dict:[5,4,1,""],generate_ddf_schema:[5,4,1,""],get_ddf_schema:[5,4,1,""],load_ddf:[5,4,1,""],to_dict:[5,4,1,""]},"ddf_utils.model.package.DataPackage":{from_dict:[5,4,1,""],from_json:[5,4,1,""],from_path:[5,4,1,""],to_dict:[5,4,1,""]},"ddf_utils.model.package.Resource":{from_dict:[5,4,1,""],to_dict:[5,4,1,""]},"ddf_utils.model.package.TableSchema":{common_fields:[5,4,1,""],field_names:[5,4,1,""],from_dict:[5,4,1,""]},"ddf_utils.model.repo":{Repo:[5,3,1,""],is_url:[5,1,1,""]},"ddf_utils.model.repo.Repo":{local_path:[5,4,1,""],name:[5,4,1,""],show_versions:[5,4,1,""],to_datapackage:[5,4,1,""]},"ddf_utils.model.utils":{absolute_path:[5,1,1,""],sort_json:[5,1,1,""]},"ddf_utils.package":{create_datapackage:[0,1,1,""],get_datapackage:[0,1,1,""],get_ddf_files:[0,1,1,""],is_datapackage:[0,1,1,""]},"ddf_utils.patch":{apply_patch:[0,1,1,""]},"ddf_utils.qa":{avg_pct_chg:[0,1,1,""],compare_with_func:[0,1,1,""],dropped_datapoints:[0,1,1,""],max_change_index:[0,1,1,""],max_pct_chg:[0,1,1,""],new_datapoints:[0,1,1,""],nrmse:[0,1,1,""],rmse:[0,1,1,""],rval:[0,1,1,""]},"ddf_utils.str":{fix_time_range:[0,1,1,""],format_float_digits:[0,1,1,""],format_float_sigfig:[0,1,1,""],to_concept_id:[0,1,1,""]},"ddf_utils.transformer":{extract_concepts:[0,1,1,""],merge_keys:[0,1,1,""],split_keys:[0,1,1,""],translate_column:[0,1,1,""],translate_header:[0,1,1,""],trend_bridge:[0,1,1,""]},ddf_utils:{"package":[0,0,0,"-"],cli:[0,0,0,"-"],i18n:[0,0,0,"-"],io:[0,0,0,"-"],patch:[0,0,0,"-"],qa:[0,0,0,"-"],str:[0,0,0,"-"],transformer:[0,0,0,"-"]}},objnames:{"0":["py","module","Python module"],"1":["py","function","Python function"],"2":["py","exception","Python exception"],"3":["py","class","Python class"],"4":["py","method","Python method"],"5":["py","attribute","Python attribute"]},objtypes:{"0":"py:module","1":"py:function","2":"py:exception","3":"py:class","4":"py:method","5":"py:attribute"},terms:{"03cf30ab":6,"13mb":6,"582kb":6,"abstract":[2,4,5,10],"case":[0,3,10,11],"class":[2,3,4,5,6,10,11],"default":[0,1,3,10,11],"export":11,"final":10,"float":4,"function":[0,1,2,3,4,6,8,10,11],"import":[6,10],"int":[0,1,3,4,10],"new":[0,1,2,3,8,10],"public":[4,6],"return":[0,1,2,3,5,10,11],"short":10,"static":[2,5],"switch":[],"true":[0,1,3,4,10],"try":[1,4,8,10],And:[6,10],For:[3,7,10,11],IDs:6,Such:1,The:[0,1,2,3,4,5,6,7],There:[0,10],Use:[7,10],With:10,_debug:10,_make_queri:4,aagr:1,abc:[2,4,5],about:[6,10],abov:[3,8],absolut:10,absolute_path:5,accept:[1,3,10],access:10,accessopt:6,accord:[0,2,10],account:6,acronym:6,act:2,actual:[0,6],acycl:2,add:[2,3,7],add_config:2,add_depend:2,add_dish:2,add_downstream:2,add_ent:5,add_ingredi:2,add_metadata:2,add_nod:2,add_procedur:2,add_upstream:2,added:2,adding:10,addit:0,adi:6,advantag:10,afr:[3,10],after:[3,10],again:10,against:10,age:[6,10],aggreagt:3,aggreg:[3,6,10],aggrgrat:3,algorithm:2,align:[0,10],all:[0,1,2,3,4,6,8,10,11],allow:10,alphanumer:0,alreadi:2,also:[3,4,8,10,11],alt_5:10,altern:10,alternative_1:10,alternative_2:10,alternative_3:10,alternative_4_cdiac:10,alternative_nam:0,ambigu:[0,3,10],among:3,amount:0,anchor:[],anhr:6,ani:[3,10],annua:6,annual:[1,6],anoth:0,answer:10,anyth:10,anywher:6,api:[0,2,4,6,7],apiaccessurl:6,app:[],append:[0,10],appli:[0,1,3,8,10],apply_patch:0,arb1:10,arb2:10,arb3:10,arb4:10,arb5:10,arb6:10,arg:[2,4,8],argument:[0,1,2,3],arrai:6,artifici:0,ascii:2,ashx:4,ask:[0,1],assum:[3,10,11],attribut:[0,1],author:[0,4,10],avail:[0,1,2,4,6,7],available_scop:1,avali:[0,3],averag:[0,1,6,11],avg_pct_chg:[0,11],avoid:0,back:[0,3,10],backoff:4,backoff_factor:4,bar:4,base:[0,1,2,3,4,5,8,10],base_df:0,base_dir:[1,2],base_path:5,base_ref:11,basenod:2,basic:7,baz:[3,10],beauti:10,becaus:[10,11],been:2,befor:10,begin:10,behavior:[3,10],being:10,belong:5,below:[3,10],benefit:10,between:[0,1,2,10],bigger:1,block:[1,3,10],bool:[0,1,3,4,10],both:10,bridg:[0,3,8,10],bridge_data:0,bridge_end:[3,10],bridge_length:[0,3,10],bridge_on:[3,10],bridge_start:[3,10],build:[0,1,3,10,11],build_dictionari:1,build_dictionary_from_datafram:1,build_dictionary_from_fil:1,build_recip:[8,10],bulk:[4,6],bulk_download:[4,6],bulkdownload:4,calcul:[1,3,10,11],call:10,callabl:0,can:[0,2,3,5,6,8,10,11],capita:[6,10],catalog:6,cattl:6,cattlepercapita_compact:6,caus:[0,6],cdiac:[],cdiacload:[],center:[3,10],chang:[0,10,11],check:[0,2,3,4,6,7,11],chef:[0,7,8,9,10],chef_opt:2,chefruntimeerror:1,childmort:[],chn:[2,10],choos:[0,1,10],classmethod:[2,5],clean:[8,11],cleanup:[0,8],cli:[7,9],clio:[4,7],clio_infra:[0,6,9],clioinfraload:[4,6],cls:2,cme:[],co2:[],cod:0,code:[4,10],coeffici:11,cofog:6,cog:0,col1:[3,10],col2:[3,10],col3:[3,10],col_a:[3,10],col_b:[3,10],collect:[2,10],column1:3,column2:3,column:[0,1,2,3,8,10,11],column_name_to_filt:[],column_to_aggreg:3,colunm:10,com:[6,8],come:[0,10],comma:10,command:[7,10,11],commandlin:8,commit:11,common:[0,8,9],common_field:5,commonli:1,comp_df:0,compar:[0,7],compare_with_func:0,comparis:10,complet:[4,8,10],complex:10,comput:[2,10],concept1:10,concept2:10,concept:[0,2,3,5,10],concept_1:[2,3,10],concept_2:[2,3,10],concept_description_1:[2,10],concept_description_2:[2,10],concept_id:5,concept_in_result:[3,10],concept_nam:3,concept_name_1:[2,10],concept_name_2:[2,10],concept_name_wildcard:3,concept_new_data:[3,10],concept_old_data:[3,10],concept_typ:[2,5,10],conceptingredi:[2,3],cond:1,condit:1,config:[2,7],configur:10,congo:0,connect:10,consist:2,consumpt:10,contain:[0,2,3,4,5,6,10],content:7,context:[4,6],continu:10,control:10,conveni:0,convert:0,cook:[2,7,8],cookbook:[7,8],copi:[0,2],copy_1_1:[],copy_2_1:[],copy_2_2:[],correl:11,correspond:10,could:4,countri:[0,3,6,10],country_cod:10,creat:[0,1,2,3,5,6,7,8,10],create_datapackag:[0,8],create_dsk:1,cropland:6,croplandpercapita_compact:6,crowdin:8,csv:[0,2,4,6,7,8,10],csvs_to_ddf:0,current:[2,6,10,11],custom:[4,7],custom_column_ord:1,custom_procedur:10,cycl:2,daff:[0,8],dag:[0,1,3],dai:[3,10],daili:8,dask:[1,5],daskdatapoint:5,data:[0,1,2,3,4,5,7,8,10],data_bridg:[3,10],data_comput:2,data_ingredi:[3,10],data_typ:[4,6],data_url_tmpl:4,datacatalog:4,datafactori:4,datafram:[0,1,2,5],datapackag:[0,5,8,10],datapoint:[0,2,3,5,10,11],datapointingredi:[2,3],dataset1:[0,11],dataset2:[0,11],dataset:[0,2,4,5,6,7,8,10],dataset_path:[2,11],datastructure_url_tmpl:4,date:[4,10],ddf:[0,2,7,8,9],ddf_dir:[1,10],ddf_example_dataset:[],ddf_id:2,ddf_util:[6,8,9,10],ddfcsv:[0,5,8],ddfschema:[0,5],debug:[1,8,10],debugg:[1,10],decemb:6,decid:10,decim:10,decl:8,deep:[3,10],def:[3,10],defin:[2,7],definit:[1,2,3,10],definitioin:[],democrat:0,depand:0,depend:[2,10],describ:[0,3,10],descript:[0,2,10],design:[8,10],detail:[8,10],detect:2,detect_cycl:2,detect_missing_depend:2,develop:[6,10],df_:0,dfs:0,dhall:7,dialect:10,dic:4,dict:[0,1,2,3,5,10],dict_def:1,dict_kei:6,dictionari:[0,1,2,3,4,5,6,10],dictionary_dir:10,dictionary_f:10,dictionary_typ:0,diff:[0,8,11],differ:[8,10,11],digit:[0,10],dimens:[3,4,5,6,10],dir:[0,1,10],direct:2,directori:[0,10],discuss:[0,10],dish:7,dishes_to_disk:10,disk:[2,5,10],displai:4,dive:10,divid:[8,10],doc:[0,2,4,6],docid:0,docstr:6,document:[6,8,10],doesn:[3,4,10],domain:[5,8,10],don:[3,4,10],done:10,download:[0,4,7],download_csv:0,download_link:4,downstream:2,downstream_list:2,downstream_node_id:2,draft:[7,8],drop:[0,3,10],dropped_datapoint:[0,11],dry:10,dsk_to_panda:1,dsl:[8,10],dtype:[2,5,6],dump:[0,5],dump_json:0,duplic:3,each:[2,3,8,10,11],easier:10,easili:[5,10],econom:6,either:10,element:10,els:0,email:6,emb:10,embed:10,empti:[3,10],enabl:10,encount:8,end:[3,10],endpoint:4,energi:10,ent:5,entir:10,entiti:[0,2,3,5,10],entity_1:[3,10],entity_2:[3,10],entity_domain:10,entity_domain_to_categor:5,entity_set:[3,5,10],entity_set_to_categor:5,entitydomain:5,entityingredi:2,entri:3,eo78_main:6,equal:11,error:[0,1,2,3,10,11],eset:5,ess:[],essenti:2,etc:[6,8],etl:[7,8,10],eval:2,evalu:2,everi:[2,3,10],exampl:[0,2,3,6,10,11],excel:0,except:[0,9],exclud:[0,10],exclude_concept:0,execut:7,executor:10,exist:[0,2,3,10],exit:8,exmapl:[3,10],expand:[3,8,10],expenditur:6,explain:10,extern:[2,10],external_concept:[2,10],extract:[0,3,10],extract_concept:[0,3,7],facil:6,factori:[0,6,7,9],fail:[0,8],fals:[0,1,2,3,5,10],femal:10,few:[6,8],field:[0,5,10],field_nam:5,file:[0,1,2,3,4,6,7,8,10],file_path:[1,2],filenam:10,filepath:[3,4],filter:[2,3,7],filter_item:10,filter_row:[2,10],find:[6,8],fingertip:10,finish:10,first:7,firstli:10,fix_time_rang:0,flag:11,flatten:[3,7],flatten_dimens:[3,10],fly:[2,10],fns:0,folder:[10,11],follow:[0,2,10,11],foo:[3,10],form:[8,10],format:[0,1,2,3,4,6,8,10,11],format_float_digit:0,format_float_sigfig:0,formatt:0,found:[0,3,10],frictionlessdata:0,from:[0,1,2,3,4,7,8,10],from_csv:[8,11],from_dict:5,from_json:5,from_path:5,from_procedure_result:2,from_recip:2,from_record:5,ftp:[],full:[2,4],full_out:[0,3,10],func:[1,2],func_nam:3,func_name1:3,func_name2:3,functioin:0,gapmind:[0,8,10],gbd:[4,7],gbd_2017_data:6,gem:6,gen_queri:1,gen_schema:0,gen_sym:1,gender:10,gener:[1,3,4,7,8,11],generate_ddf_schema:5,geo:[0,2,3,10,11],geo_entity_domain:10,geo_nam:10,geo_new:10,geograph:0,get:[0,1,2,3,4,5,10],get_data:2,get_data_from_ddf_dataset:2,get_data_from_external_csv:2,get_data_from_inline_data:2,get_datapackag:0,get_datapoint:5,get_ddf_fil:0,get_ddf_schema:5,get_direct_rel:2,get_ent:5,get_entity_set:5,get_ingredient_class:2,get_nod:2,get_procedur:1,get_synonym:5,getdatastructur:4,getkeyfamili:4,ghdx:[4,6],gist:10,git:[8,11],github:8,give:8,given:[0,1,4,10],global:[3,10],goat:6,goatspercapita_compact:6,god_id:10,going:10,good:[2,10],googl:0,gov:[],govern:6,graph:2,group:[3,6,10],groupbi:[1,3,7],growth:1,gte:10,guidelin:7,handl:[0,3],has:[6,10],has_ent:5,has_newer_sourc:[4,6],has_nod:2,hash:4,have:[0,1,2,3,4,5,10,11],head:6,head_ref:11,header:[0,3,5,10],healthdata:[4,6],help:[6,8,10],helper:[0,7,9,10],here:[0,2,10],hierarchi:4,higher:[0,1],home:[6,10],hour:6,how:[0,1,3,6,8,10,11],howev:4,html:4,http:[0,4,6,8],human:[],hy_mod:10,i18n:[7,8,9],id_of_new_ingredi:10,igm:[],igmeload:[],ignore_cas:[0,1,3],ihm:[0,7,9],ihme_query_tool:4,ihmeload:[4,6],ilo:[0,6,9],iloload:4,ilostat:[4,7],improv:[10,11],includ:[0,2,3,4,7,8],include_eq:1,include_kei:[3,10],include_low:1,include_upp:1,index:[0,1,3,4,7,10],indic:[0,3,4,5,6,10,11],indicator1:11,indicator2:11,indicator_meta_url_tmpl:4,infer:[],info:7,inform:[3,10],infra:[4,7],ingledi:10,ingredi:[0,1,3,7],ingredient_from_dict:2,ingredient_id:[3,10],ingredient_id_1:[3,10],ingredient_id_2:[3,10],ingredient_id_3:[3,10],ingredient_to_rol:3,ingredient_to_run:[3,10],ingredient_to_run_the_proc:10,ingredient_to_serv:10,ingredient_to_serve_1:10,ingredient_to_serve_2:10,ingredient_typ:2,ingredienterror:1,ingredientnod:2,ingredients_out:[0,3,10],inherit:2,init:10,initi:10,inlin:[0,2,10],input:[0,3,10,11],input_file_or_path:11,insert:3,insert_kei:[3,10],insid:10,instal:[7,10],instanc:3,interfac:7,intermedi:7,intern:10,introduct:7,invalid:10,is_datapackag:0,is_url:5,iso3166_1_alpha2:10,iso3166_1_alpha3:10,iso:10,issu:0,item:[3,10],iter:0,its:[2,4,10],itself:10,javascript:[],join:[0,3,10],json:[0,4,5,6,8,10],json_path:5,judg:0,jump:0,just:[0,3,6,10],keep:[0,2,3,10],keep_decim:0,kei:[0,1,2,3,6,10,11],kept:10,key_col_1:[],key_col_2:[],key_to_list:2,keyword:[0,1,2,3,10],keywork:4,kind:[6,10],kwarg:[0,2,4],lang:[0,4],lang_path:0,langsplit:0,languag:[4,10],last:[4,10],later:[3,10,11],latest:8,lbl:[],learn:[6,10],length:0,let:10,letter:10,level:[3,10],librari:7,licens:[0,10],like:[0,1,6,10,11],limit:0,line:[7,10],link:[4,8],lisp:10,list:[0,1,2,3,4,5,6,10],load:[4,5,6,10],load_ddf:5,load_metadata:[4,6],loader:7,local:2,local_path:5,locat:6,logic:10,longer:[],look:[0,10],loop:10,lower:[1,10],lte:10,macro:10,made:10,mai:[6,10],mailer:6,main:[2,10],main_url:4,mainli:[],make:[2,4,10],male:10,manag:0,mani:10,manipul:[8,10],manual:[0,3,10],map:[0,3,10],match:[3,10],math:[3,10],matter:10,max:11,max_change_index:0,max_pct_chg:[0,11],maximum:[0,11],mdg:6,mean:[0,1,3,5,10,11],measur:[2,6,10],mention:[1,3,10],merg:[0,3,7,8],merge_ent:[3,7],merge_kei:0,merge_transl:8,merge_translations_csv:0,merge_translations_json:0,messag:8,meta:10,metadata:[0,2,4,6,10],metadata_url:4,method:[0,1,2,4,6],metric:6,middl:0,might:10,min:11,min_period:[3,10],mind:10,miss:2,mit:10,mix:[3,10],mkfunc:1,mock:[0,1,2],mode:7,model:[0,1,3,7,8,9,10],modifi:[3,4],modul:[3,6,7,8,9,10],mongo:[1,10],mongodb:10,more:[0,3,6,10],most:10,multiindex:0,multipl:[3,4,10],must:[0,3,4,10],my_fancy_dataset:10,name:[0,1,2,3,5,6,8,10],namespac:2,nan:6,nation:6,ndp030:[],necessari:2,need:[3,4,6,10,11],new_col_nam:[3,10],new_column_nam:[],new_concept_name_templ:3,new_data:0,new_data_ingredi:[3,10],new_datapoint:[0,11],new_ingredient_id:[3,10],new_nam:0,new_name_:10,new_ser:0,newer:[4,6],newnam:[3,10],next:10,nin:10,no_keep_set:10,node:2,node_dict:2,node_id:2,nodej:10,none:[0,1,2,3,4,5],nor:10,not_found:[0,3,10],note:[3,4,10,11],noth:5,notic:10,now:[8,10,11],nrmse:[0,11],number:[0,3,8],numer:[6,10],numpi:1,obj:0,object:[0,1,2,4,5,6,10],observ:4,oecd:[0,7,9],oecdload:[4,6],oil:10,oil_consumption_per_capita:10,oil_consumption_tonn:10,oil_per_person:10,okai:10,old:[0,3,10],old_data:0,old_data_ingredi:[3,10],old_name_wildcard:10,old_ser:0,oldnam:[3,10],omit:10,onc:2,one:[0,3,5,6,10,11],ones:[3,10],onli:[0,2,3,5,10,11],open:4,open_google_spreadsheet:0,oper:[3,10],ops:[0,9],opt:10,optino:[3,10],option:[0,1,2,3,4,7,8],order:[1,3,8,10],org:[4,6],other:[1,2,3,8,10],other_meta_url_tmpl:4,our:10,out:6,out_dir:[0,1,4,10],out_fil:4,out_path:[0,11],outdir:10,outlook:6,outpath:2,output:[3,4,10],output_dir:10,output_ingredi:[3,10],over:10,overwirt:10,overwrit:[0,3,10],overwritten:[3,10],own:10,packag:[7,8,9,10],page:[6,7,10],panda:[0,1,2,3,5,10],pandasdatapoint:5,pandg:10,param1:[3,10],param2:3,paramet:[0,1,2,3,4,6,10],pars:6,part:[0,1,10],pass:[0,2,10],pastur:6,pasturepercapita_compact:6,pat_ind:6,patch:[7,8,9],patent:6,path:[0,1,2,4,5,10,11],path_to_dataset:10,path_to_ddf_dir:10,path_to_dict_dir:10,path_to_recip:10,path_to_rsecip:10,path_to_your_dataset:10,peopl:8,per:[6,10],percentag:11,perform:10,person:10,perviou:10,php:4,pig:6,pigspercapita_compact:6,pip3:8,pip:8,pkei:5,place:[0,10],plain:10,pleas:[4,8,10,11],plot:6,plug:5,pool_siz:4,pop:10,popul:10,population_by_gender_ingredi:10,population_femal:10,population_mal:10,population_tot:[3,10],portal:[],possibl:6,post:[],precentag:0,predefin:10,present:[3,10],pretti:10,previou:3,primari:[10,11],primarykei:[2,3,5,10],print:10,problem:6,proc_nam:10,proce:11,procedur:[0,1,2,7],procedureerror:1,procedurenod:2,procedureresult:10,procedures_dir:10,process:[8,10],processor:10,profil:6,progress:4,progress_bar:4,project:[0,8,10],project_root:10,prologu:7,prompt:[0,3,10],prompt_select:1,prop:5,properti:[2,5,6],protocol:2,provid:[0,1,3,7,8,10,11],put:[0,10,11],pypi:8,python:[0,8,10],qna:6,quarterli:6,queri:[1,4,6,10],question:10,quot:10,rais:[1,2],rang:0,rank:1,rate:1,raw:0,read:[0,1,8,10],read_opt:1,readi:10,recip:[1,2,3,7,8],recipe_fil:[2,10],recipes_dir:10,recommend:10,recur:10,ref:5,refer:[7,10],register_procedur:2,rei:6,rel:[0,2,10],relat:[3,10],remov:0,renam:3,replac:[0,2,3],repo:[0,9,10,11],report:[8,10],repositori:5,represent:2,republ:0,request:[4,6],requests_retry_sess:4,requir:[1,2,8,10],res:10,reserv:10,reslut:0,resourc:[0,5],restsdmx:4,result:[0,1,2,3,4,6,7],resum:4,retain:1,retri:4,retry_tim:4,revers:1,rmse:[0,11],roll:[1,3,10],root:[0,2,11],row:[1,2,3,8,10],row_filt:2,run:[1,2,3,4,7,8,11],run_op:[3,7],run_recip:[1,8,10],rval:[0,11],same:[5,6,10],save:[0,10],schema:[0,5,7],scope:1,scrape:4,script:[0,10,11],sdmx:[4,6],search:[1,4,6,7,10],section:[0,7],see:[0,2,3,4,6,8,10],select:[1,2,4,6,10],self:2,semio:8,send:2,sep:0,separ:10,seper:10,sequela:6,seri:0,serv:[1,2,7],serval:6,serve_concept:0,serve_datapoint:0,serve_ent:0,session:4,set:[0,2,3,5,7,8,11],setuptool:8,sever:10,sex:6,short_nam:6,should:[0,1,2,3,6,10,11],show:[2,8],show_vers:5,shown:10,shuld:3,sid:5,sigfig:0,signific:0,similar:10,simpl:2,simpli:10,simultan:4,sinc:10,singl:0,site:4,size:[1,3,10],skip:[0,3,10],skip_totals_among_ent:3,smooth:[0,10],sna_table11:6,some:[0,3,10],some_measure_concept:[],someth:2,sometim:[0,10],sort:[1,5,6],sort_df:1,sort_json:5,sort_key_column:1,sourc:[0,4,7,8,10],source_dataset:10,spec:0,specif:[6,10],specifi:[4,10,11],split:[0,3,8,10],split_ent:[3,7],split_kei:0,split_path:0,split_transl:8,split_translations_csv:0,split_translations_json:0,splite:0,spreadsheet:0,squar:11,stai:[3,10],stair:0,standard:[0,11],start:[3,10],stat:[4,6],statement:10,statist:[8,11],statu:6,status_forcelist:4,step:[3,10],still:10,store:[0,2,3,5,10],str:[1,2,3,4,5,7,8,9,10],string:[0,1,2,3,4,8,10],structur:7,style:[3,10],sub:[1,10],submodul:[7,9],subpackag:[7,9],sum:[3,10],suppli:3,support:[1,3,6,8,10,11],suppos:10,swe:[2,10],symbol:[1,10],synonym:[5,10],syntax:10,sys:10,system:10,systemat:0,tabl:[0,4,5],table_of_contents_:4,tableschema:5,tabular:[0,8],tail:0,take:10,talk:10,target:[3,10],target_column:[0,3,10],tarjan:2,task:[0,2,7,8,10],tbd:[],tell:10,templat:[3,10],test:2,text_befor:1,than:[0,4],thei:[3,4,10],them:[0,3,6,10],thi:[0,1,2,3,5,6,8,10,11],thing:[4,10],though:10,threshold:0,time:[0,2,3,4,10],titl:0,tmp:6,to_concept_id:0,to_csv:0,to_datafram:5,to_datapackag:5,to_dict:5,to_graph:2,to_json:0,to_recip:[2,10],togeth:10,too:[10,11],tool:[4,6,8,10],top:10,total:3,transform:[3,7,8,9,10],translat:[0,3,8,10],translate_column:[0,3,7],translate_head:[0,3,7],treat:[],tree:[2,10],tree_view:2,trend:[3,8],trend_bridg:[0,3,7],tri:6,tupl:5,turn:5,two:[2,3,10],type:[0,1,2,3,6,10],ues_default_exclud:0,under:[6,10],union:[0,2,3,5,10],uniqu:6,unpop:10,updat:[0,5,8],upper:1,upper_case_nam:10,upstream:2,upstream_list:2,upstream_node_id:2,uri:5,url:[0,4,6],url_data:4,url_hir:4,url_metadata:4,url_task:4,url_vers:4,usa:[2,3,10],usag:[6,7,10],use:[0,3,4,6,8,10,11],use_exist:0,used:[0,1,3,4,10],useful:[3,8,11],user:[0,1,7],uses:4,using:[2,3,4,7,10],util:[0,1,2,4,8,9,10],val:[1,10],valiat:10,valid:[2,7,8],validate_recip:[8,10],valu:[0,1,2,3,5,6,10],value_modifi:[1,3],variabl:[3,10],variou:8,ver:4,veri:11,version:[2,4,6,8,10],wai:[2,6,10],want:[0,3,4,10],warpper:1,wdi:6,web_bulk_download:4,websit:[4,6],weight:1,well:[10,11],what:[0,3,7],whehter:1,when:[0,2,3,4,8,10,11],whenev:6,where:[2,10,11],whether:[0,4],which:[0,1,2,3,5,6,10,11],whole:[3,10],wildcard:[3,10],window:[1,3,7],wip:10,without:10,wonder:10,work:[0,6,8,10],worker:6,workflow:0,world:10,worldbank:[0,7,9],worldbankload:[4,6],would:[6,10],wrapper:4,write:[0,2,3,7,8],writer:10,written:10,wrong:10,www:4,xlsx:6,xxxx:6,xxxx_file:6,yaml:[2,10],year:[0,3,6,10],year_rang:6,yield:0,yml:10,you:[0,4,8,10,11],your:[6,7],your_compani:10,zcore:1,zero:0,zip:6},titles:["ddf_utils package","ddf_utils.chef package","ddf_utils.chef.model package","ddf_utils.chef.procedure package","ddf_utils.factory package","ddf_utils.model package","Downloading Data from Source Providers","Welcome to ddf_utils\u2019s documentation!","Introduction","API References","Recipe Cookbook (draft)","Use ddf_utils for ETL tasks"],titleterms:{For:8,The:10,Use:11,add:10,api:[1,9],avail:[3,10],basic:10,cdiac:[],check:10,chef:[1,2,3],cli:0,clio:6,clio_infra:4,command:8,common:4,compar:11,config:10,cook:10,cookbook:10,copi:[],creat:11,csv:11,custom:10,dag:2,data:[6,11],dataset:11,ddf:[5,10,11],ddf_util:[0,1,2,3,4,5,7,11],defin:10,dhall:10,dish:10,document:7,download:6,draft:10,etl:11,except:1,execut:10,extract_concept:10,factori:4,file:11,filter:10,filter_item:[],filter_row:[],first:10,flatten:10,from:[6,11],gbd:6,gener:[6,10],groupbi:10,guidelin:10,helper:[1,8],i18n:0,ident:[],igm:[],ihm:[4,6],ilo:4,ilostat:6,includ:10,indic:7,info:10,infra:6,ingredi:[2,10],instal:8,interfac:6,intermedi:10,introduct:8,librari:8,line:8,loader:6,merg:10,merge_ent:10,mode:10,model:[2,5,11],modul:[0,1,4,5],oecd:[4,6],ops:1,option:10,packag:[0,1,2,3,4,5],patch:0,procedur:[3,10],prologu:10,provid:6,recip:10,refer:9,repo:5,result:10,run:10,run_op:10,schema:10,section:10,serv:10,set:10,sourc:6,split_ent:10,str:0,structur:10,submodul:[0,1,4,5],subpackag:[0,1],tabl:7,task:11,transform:0,translate_column:10,translate_head:10,trend_bridg:10,usag:8,user:8,using:11,util:5,valid:10,welcom:7,what:10,window:[8,10],worldbank:[4,6],write:10,your:10}})
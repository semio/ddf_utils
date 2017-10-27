Search.setIndex({docnames:["__recipe","ddf_utils","ddf_utils.chef","ddf_utils.model","index","intro","modules","recipe","utils"],envversion:53,filenames:["__recipe.md","ddf_utils.rst","ddf_utils.chef.rst","ddf_utils.model.rst","index.rst","intro.rst","modules.rst","recipe.rst","utils.rst"],objects:{"ddf_utils.chef":{api:[2,0,0,"-"],cook:[2,0,0,"-"],dag:[2,0,0,"-"],helpers:[2,0,0,"-"],ingredient:[2,0,0,"-"],ops:[2,0,0,"-"],procedure:[2,0,0,"-"]},"ddf_utils.chef.cook":{Chef:[2,1,1,""]},"ddf_utils.chef.cook.Chef":{add_config:[2,2,1,""],add_ingredient:[2,2,1,""],add_metadata:[2,2,1,""],add_procedure:[2,2,1,""],config:[2,3,1,""],copy:[2,2,1,""],from_recipe:[2,4,1,""],ingredients:[2,3,1,""],register_procedure:[2,5,1,""],run:[2,2,1,""],serving:[2,3,1,""],to_graph:[2,2,1,""],to_recipe:[2,2,1,""],validate:[2,2,1,""]},"ddf_utils.chef.dag":{BaseNode:[2,1,1,""],DAG:[2,1,1,""],IngredientNode:[2,1,1,""],ProcedureNode:[2,1,1,""]},"ddf_utils.chef.dag.BaseNode":{add_downstream:[2,2,1,""],add_upstream:[2,2,1,""],detect_downstream_cycle:[2,2,1,""],detect_missing_dependency:[2,2,1,""],downstream_list:[2,3,1,""],evaluate:[2,2,1,""],get_direct_relatives:[2,2,1,""],upstream_list:[2,3,1,""]},"ddf_utils.chef.dag.DAG":{add_dependency:[2,2,1,""],add_node:[2,2,1,""],copy:[2,2,1,""],get_node:[2,2,1,""],has_node:[2,2,1,""],node_dict:[2,3,1,""],nodes:[2,3,1,""],roots:[2,3,1,""],tree_view:[2,2,1,""]},"ddf_utils.chef.dag.IngredientNode":{evaluate:[2,2,1,""]},"ddf_utils.chef.dag.ProcedureNode":{evaluate:[2,2,1,""]},"ddf_utils.chef.helpers":{debuggable:[2,6,1,""],gen_query:[2,6,1,""],gen_sym:[2,6,1,""],get_procedure:[2,6,1,""],log_dtypes:[2,6,1,""],log_procedure:[2,6,1,""],log_shape:[2,6,1,""],mkfunc:[2,6,1,""],prompt_select:[2,6,1,""],query:[2,6,1,""],read_opt:[2,6,1,""]},"ddf_utils.chef.ingredient":{BaseIngredient:[2,1,1,""],Ingredient:[2,1,1,""],ProcedureResult:[2,1,1,""]},"ddf_utils.chef.ingredient.BaseIngredient":{copy_data:[2,2,1,""],dtype:[2,3,1,""],get_data:[2,2,1,""],key_to_list:[2,2,1,""],reset_data:[2,2,1,""],serve:[2,2,1,""]},"ddf_utils.chef.ingredient.Ingredient":{data:[2,3,1,""],dataset_path:[2,3,1,""],ddf:[2,3,1,""],ddf_id:[2,3,1,""],from_dict:[2,4,1,""],get_data:[2,2,1,""],ingred_id:[2,3,1,""],key:[2,3,1,""],row_filter:[2,3,1,""],values:[2,3,1,""]},"ddf_utils.chef.ops":{aagr:[2,6,1,""],between:[2,6,1,""],gt:[2,6,1,""],lt:[2,6,1,""],zcore:[2,6,1,""]},"ddf_utils.chef.procedure":{extract_concepts:[2,6,1,""],filter:[2,6,1,""],filter_item:[2,6,1,""],filter_row:[2,6,1,""],flatten:[2,6,1,""],groupby:[2,6,1,""],merge:[2,6,1,""],merge_entity:[2,6,1,""],run_op:[2,6,1,""],split_entity:[2,6,1,""],translate_column:[2,6,1,""],translate_header:[2,6,1,""],trend_bridge:[2,6,1,""],window:[2,6,1,""]},"ddf_utils.datapackage":{create_datapackage:[1,6,1,""],dump_json:[1,6,1,""],get_datapackage:[1,6,1,""],get_ddf_files:[1,6,1,""]},"ddf_utils.i18n":{merge_translations_csv:[1,6,1,""],merge_translations_json:[1,6,1,""],split_translations_csv:[1,6,1,""],split_translations_json:[1,6,1,""]},"ddf_utils.io":{cleanup:[1,6,1,""],csvs_to_ddf:[1,6,1,""],download_csv:[1,6,1,""],load_google_xls:[1,6,1,""],to_csv:[1,6,1,""]},"ddf_utils.model":{"package":[3,0,0,"-"],ddf:[3,0,0,"-"]},"ddf_utils.model.ddf":{Dataset:[3,1,1,""]},"ddf_utils.model.ddf.Dataset":{concepts:[3,3,1,""],datapoints:[3,3,1,""],domains:[3,3,1,""],entities:[3,3,1,""],get_data_copy:[3,2,1,""],get_datapoint_df:[3,2,1,""],get_entity:[3,2,1,""],indicators:[3,2,1,""],is_empty:[3,3,1,""],rename:[3,2,1,""],to_ddfcsv:[3,2,1,""],validate:[3,2,1,""]},"ddf_utils.model.package":{Datapackage:[3,1,1,""]},"ddf_utils.model.package.Datapackage":{concepts_resources:[3,3,1,""],datapoints_resources:[3,3,1,""],dataset:[3,3,1,""],dump:[3,2,1,""],entities_resources:[3,3,1,""],generate_ddfschema:[3,2,1,""],load:[3,2,1,""],name:[3,3,1,""],resources:[3,3,1,""]},"ddf_utils.patch":{apply_patch:[1,6,1,""]},"ddf_utils.qa":{avg_pct_chg:[1,6,1,""],compare_with_func:[1,6,1,""],max_change_index:[1,6,1,""],max_pct_chg:[1,6,1,""],min_pct_chg:[1,6,1,""],rval:[1,6,1,""]},"ddf_utils.str":{fix_time_range:[1,6,1,""],format_float_digits:[1,6,1,""],format_float_sigfig:[1,6,1,""],to_concept_id:[1,6,1,""]},"ddf_utils.transformer":{extract_concepts:[1,6,1,""],merge_keys:[1,6,1,""],split_keys:[1,6,1,""],translate_column:[1,6,1,""],translate_header:[1,6,1,""],trend_bridge:[1,6,1,""]},ddf_utils:{cli:[1,0,0,"-"],datapackage:[1,0,0,"-"],i18n:[1,0,0,"-"],io:[1,0,0,"-"],patch:[1,0,0,"-"],qa:[1,0,0,"-"],str:[1,0,0,"-"],transformer:[1,0,0,"-"]}},objnames:{"0":["py","module","Python module"],"1":["py","class","Python class"],"2":["py","method","Python method"],"3":["py","attribute","Python attribute"],"4":["py","classmethod","Python class method"],"5":["py","staticmethod","Python static method"],"6":["py","function","Python function"]},objtypes:{"0":"py:module","1":"py:class","2":"py:method","3":"py:attribute","4":"py:classmethod","5":"py:staticmethod","6":"py:function"},terms:{"case":[0,2,7],"class":[2,3,7],"default":[0,1,2,7],"final":7,"function":[0,1,2,5,7],"import":[0,7],"int":[0,1,2,7],"new":[0,1,2,5,7],"return":[0,1,2,3,7],"short":7,"static":2,"true":[0,1,2,3,7],"try":[0,2,5,7],And:7,For:[0,2,7],Such:2,The:[0,1,2,3,7],There:7,Use:7,With:7,_debug:7,aagr:2,about:[0,7],abov:[0,2,5],absolut:[0,7],accept:[0,2,7],access:[0,7],accord:[0,2,7],act:2,actual:1,acycl:2,add:[0,2],add_config:2,add_depend:2,add_downstream:2,add_ingredi:2,add_metadata:2,add_nod:2,add_procedur:2,add_upstream:2,added:2,afr:[0,2,7],after:7,age:7,aggreagt:2,aggreg:[0,2,7],aggrgrat:2,align:[0,1,7],all:[0,1,2,5,7],allow:[0,7],alphanumer:1,alreadi:2,also:[0,2,5,7],alt_5:[0,7],alternative_1:[0,7],alternative_2:[0,7],alternative_3:[0,7],alternative_4_cdiac:[0,7],alternative_nam:1,ambigu:[0,1,2,7],amount:1,anchor:0,ani:[0,2,7],annual:2,anoth:1,answer:7,anyth:7,api:[1,4],append:[0,7],appli:[1,2,5,7],apply_patch:1,arb1:[0,7],arb2:[0,7],arb3:[0,7],arb4:[0,7],arb5:[0,7],arb6:[0,7],arg:[2,5],arguement:[],argument:[1,2,3],artifici:1,ascii:2,ask:[1,2],assum:[2,7],attr:3,attribut:[1,2],author:7,avail:[2,4],available_scop:2,avali:1,averag:[1,2],avg_pct_chg:1,avoid:1,back:[2,7],base:[0,1,2,3,5,7],base_df:1,base_dir:[2,3],baseingredi:2,basenod:2,basic:0,baz:[0,2,7],becaus:7,been:2,befor:7,begin:[0,7],behavior:[0,2,7],being:7,below:[0,2,7],between:[1,2,7],block:[0,2,7],bool:[0,1,2,3,7],both:[0,7],bridg:[0,1,2,5,7],bridge_data:1,bridge_end:[2,7],bridge_length:[1,2,7],bridge_on:[2,7],bridge_start:[2,7],build:[0,1,2,7],build_recip:[0,5,7],calcul:[0,2,7],call:7,can:[0,1,2,5,7],capita:7,center:[0,2,7],chang:[0,1,7],check:[0,2],chef:[0,1,5,6,7],chn:2,choos:[0,1,2,7],classmethod:2,clean:5,cleanup:[1,5],cli:6,cod:1,cog:1,col1:[0,2,7],col2:[0,2,7],col3:[0,2,7],col_a:[0,2,7],col_b:[0,2,7],collect:[2,7],column1:2,column2:2,column:[0,1,2,5,7],column_nam:7,column_name_to_filt:0,column_to_aggreg:2,colunm:[0,7],com:5,come:[0,7],comma:7,command:[0,7],commandlin:5,common:[1,5],commonli:2,comp_df:1,compar:1,compare_with_func:1,comparis:2,complet:[5,7],complex:2,comput:7,concept1:2,concept2:2,concept:[0,1,2,3,7],concept_1:[2,7],concept_2:[2,7],concept_description_1:2,concept_description_2:2,concept_in_result:[2,7],concept_nam:2,concept_name_1:2,concept_name_2:2,concept_name_wildcard:2,concept_new_data:[2,7],concept_old_data:[2,7],concept_typ:[2,7],concepts_resourc:3,cond:2,condit:2,config:2,configur:[0,7],congo:1,connect:[0,7],consist:2,construct:2,consumpt:[0,7],contain:[0,1,2,7],content:4,continu:[0,7],control:[0,7],convert:1,cook:[1,5,6],cookbook:[4,5],copi:2,copy_1_1:0,copy_2_1:0,copy_2_2:0,copy_data:2,correspond:7,countri:[0,1,2,7],country_cod:7,creat:[0,1,2,5,7],create_datapackag:[1,5],crowdin:5,csv:[0,1,2,3,5,7],csvs_to_ddf:1,current:[0,2,7],custom:[0,7],cycl:2,daff:[1,5],dag:[1,6],dai:[2,7],daili:5,data:[0,1,2,3,5,7],data_bridg:[2,7],data_def:2,data_ingredi:[2,7],datafram:[1,2,3],datapackag:[0,3,5,6,7],datapoint:[0,2,3,7],datapoints_resourc:3,dataset1:1,dataset2:1,dataset:[0,1,2,3,5,7],dataset_path:2,ddf:[0,1,2,5,6,7],ddf_dir:[0,7],ddf_example_dataset:[0,7],ddf_id:2,ddf_reader:[],ddf_util:[0,5,6,7],ddfcsv:[1,5],debug:[5,7],debugg:2,decid:7,decim:[0,7],decl:5,deep:[0,2,7],def:[0,2,7],defin:[0,2],definit:[0,2,7],definitioin:[0,7],democrat:1,depand:1,depend:[0,2,7],deprec:7,describ:[0,1,2,7],descript:[0,2,7],design:[5,7],detail:[0,2,5,7],detect:2,detect_downstream_cycl:2,detect_missing_depend:2,develop:7,dfs:1,dict:[0,1,2,7],dictionari:[0,1,2,7],dictionary_dir:[0,7],dictionary_f:7,dictionary_typ:1,diff:[1,5],differ:5,digit:[0,1,2,7],dimens:[2,7],dir:[0,2,7],direct:2,directori:[0,1,7],discuss:[0,7],dishes_to_disk:[0,7],disk:[0,2,3,7],divid:[5,7],doc:[1,2],document:[5,7],doesn:[0,2,7],domain:[0,3,5,7],don:7,done:7,download:1,download_csv:1,downstream:2,downstream_list:2,downstream_node_id:2,draft:[4,5],drop:[0,1,2,7],dry:7,dry_run:2,dsl:[0,5,7],dtype:2,dump:3,dump_json:1,each:[0,2,5,7],easier:7,easili:[0,7],either:[0,2,7],els:1,emb:[0,7],empti:[2,7],enabl:7,encount:5,end:[2,7],energi:[0,7],ent:3,entiti:[0,1,2,3,7],entities_resourc:3,entity_1:[2,7],entity_2:[2,7],entity_domain:7,entity_set:2,entri:2,error:[0,1,2,7],essenti:2,etc:[0,5],etl:[5,7],eval:2,evalu:2,everi:[0,2,7],exampl:[0,1,2,7],except:2,exclud:2,exclude_concept:1,execut:4,executor:[0,7],exist:[0,1,2,7],exit:5,exmapl:[0,2,7],expand:[0,2,5,7],explain:[0,7],extern:2,external_concept:2,extract:[0,1,2,7],extract_concept:[1,2],fail:5,fals:[0,1,2,7],femal:7,few:[3,5],field:[0,1,7],file:[0,1,2,5,7],filehash:1,filenam:[0,7],filter:[0,2],filter_col_1:2,filter_col_2:2,filter_item:2,filter_row:2,filter_val_1:2,filter_val_2:2,find:5,finish:7,first:[3,4],firstli:7,fix_time_rang:1,flatten:2,flatten_dimens:[2,7],fly:[2,7],fns:1,folder:[2,3,7],follow:[0,2,7],foo:[0,2,7],form:[5,7],format:[0,1,2,5,7],format_float_digit:1,format_float_sigfig:1,found:[0,1,2,7],frictionlessdata:1,from:[0,1,2,3,5,7],from_csv:5,from_dict:2,from_recip:2,ftype:1,full_out:[0,1,2,7],func:2,func_nam:2,func_name1:2,func_name2:2,functioin:1,gapmind:[0,1,5,7],gapminder_list:7,gen_queri:2,gen_schema:1,gen_sym:2,gender:7,gener:[2,4,5],generate_ddfschema:3,geo:[0,1,2,7],geo_entity_domain:7,geo_nam:7,geo_new:[0,7],geograph:1,get:[0,1,2,7],get_data:2,get_data_copi:3,get_datapackag:1,get_datapoint_df:3,get_ddf_fil:1,get_direct_rel:2,get_ent:3,get_nod:2,get_procedur:2,gist:7,git:5,github:[0,5],give:5,given:[1,7],global:2,god_id:[0,7],going:7,good:2,googl:1,graph:2,groubbi:2,group:[0,2,7],groupbi:2,growth:2,gte:2,guidelin:4,handl:2,has:7,has_nod:2,have:[0,1,2,7],header:[0,1,2,7],help:[5,7],helper:[1,6],here:[0,2,7],higher:1,how:[0,1,2,5,7],http:[1,5],human:0,i18n:[5,6],id_of_new_ingredi:[0,7],ignore_cas:[1,2],implement:2,improv:[0,7],includ:[1,2,5],include_eq:2,include_kei:[0,2,7],include_low:2,include_upp:2,index:[2,4,7],indic:[0,1,2,3,7],infer:2,inform:[0,2,7],ingerdi:[],ingledi:[0,2,7],ingred_id:2,ingredi:[1,6],ingredient_id:[0,2,7],ingredient_id_1:[0,2,7],ingredient_id_2:[0,2,7],ingredient_id_3:[0,2,7],ingredient_to_rol:2,ingredient_to_run:[0,2,7],ingredient_to_run_the_proc:[0,7],ingredient_to_serv:[0,7],ingredient_to_serve_1:[0,7],ingredient_to_serve_2:[0,7],ingredientnod:2,ingredients_out:[0,1,2,7],inherit:2,inlin:[1,2],input:[1,2,7],insert:2,insert_kei:[2,7],insid:[0,7],instal:4,instanc:2,intern:[0,7],introduct:4,invalid:7,invok:2,is_empti:3,iso3166_1_alpha2:[0,7],iso3166_1_alpha3:[0,7],iso3166_2:7,iso:7,issu:0,item:[0,2,7],its:[2,7],itself:[0,7],join:[0,1,2,7],json:[0,1,3,5,7],jump:1,just:[1,2,7],keep:[0,1,2,7],keep_decim:1,kei:[0,1,2,7],kept:[0,7],key_as_index:2,key_col_1:[0,7],key_col_2:[0,7],key_to_list:2,keyword:[1,2,3],kind:7,kwarg:[1,2,3],lang:1,lang_path:1,langsplit:1,languag:[0,7],last:[0,7],later:[0,2,7],latest:5,learn:[0,7],length:1,let:7,letter:7,level:[0,2,7],lib:[],licens:7,like:[0,1,2,7],limit:1,line:[0,7],link:5,list:[0,1,2,7],load:3,load_google_xl:1,local:[2,3],log_dtyp:2,log_procedur:2,log_shap:2,logic:2,look:[1,7],loop:[0,7],lower:[2,7],lte:2,made:[0,7],mai:7,main:[0,2,7],mainli:0,make:[0,2,7],male:7,manag:1,mani:[0,2,7],manipul:[0,5,7],manual:[1,2,7],map:[0,1,2,7],match:[0,2,7],math:[0,2,7],max_change_index:1,max_pct_chg:1,maximum:1,mean:[0,1,2,7],measur:2,mention:[0,2,7],merg:[1,2,5],merge_ent:2,merge_kei:1,merge_transl:5,merge_translations_csv:1,merge_translations_json:1,messag:5,meta:7,metadata:[2,7],meth:[],method:2,middl:1,might:[0,7],min_pct_chg:1,min_period:[0,2,7],mind:7,miss:2,mit:7,mix:[0,2,7],mkfunc:2,mock:[1,2],mod:[],model:[1,2,5,6,7],modul:[4,5,6],mongo:[2,7],more:[0,1,2,7],most:[0,2,7],multipl:[0,2,7],must:[0,1,2,7],name:[0,1,2,3,5,7],namespac:2,necessari:2,need:[0,2,7],new_col_nam:[0,2,7],new_column_nam:0,new_concept_name_templ:2,new_data:1,new_data_ingredi:[2,7],new_ingredient_id:[0,2,7],new_nam:1,new_name_:7,newnam:[0,2,7],next:[0,7],nin:2,no_datapoint:3,node:2,node_dict:2,node_id:2,none:[1,2,3],nor:2,not_found:[0,1,2,7],note:[0,2,7],notic:7,now:[0,2,5,7],number:[1,2,5],numer:[0,7],numpi:2,obj:1,object:[0,2,3,7],oil:[0,7],oil_consumption_per_capita:7,oil_consumption_tonn:7,oil_per_person:7,old:[0,1,2,7],old_data:1,old_data_ingredi:[2,7],old_name_wildcard:7,oldnam:[0,2,7],omit:7,onc:2,one:[0,1,2,7],ones:[0,2,7],onli:[0,1,2,3,7],oper:[0,2,7],ops:[1,6],opt:[0,7],optino:[0,2,7],option:[0,1,2,3,5],order:[0,2,5,7],other:[0,2,5,7],our:7,out_dir:[1,3,7],out_path:1,outdir:[0,7],outpath:2,output:[0,2,7],output_dir:[0,7],output_ingredi:[2,7],overwirt:[0,7],overwrit:[0,1,2,7],overwritten:[0,2,7],packag:[4,5,6,7],page:4,panda:[0,1,2,7],pandg:[0,7],param1:[0,2,7],param2:2,paramet:[0,1,2,7],part:[0,7],pass:[0,1,7],patch:[5,6],path:[0,1,2,3,7],path_to_dataset:[0,7],path_to_recip:[0,7],path_to_rsecip:[0,7],peopl:5,per:[0,7],perform:[0,7],person:[0,7],perviou:[0,7],pip3:5,pip:5,place:[0,1,7],pleas:[5,7],pop:7,popul:[0,7],population_by_gender_ingredi:7,population_femal:7,population_mal:7,population_tot:[2,7],precentag:1,present:[0,2,7],pretti:7,previou:2,primari:[0,7],primary_kei:3,primarykei:[0,2,7],print:7,proc_nam:[0,7],procedur:[1,4,6],procedurenod:2,procedureresult:2,process:[0,5,7],processor:7,project:[1,5,7],project_root:7,prompt:[0,1,2,7],prompt_select:2,properti:2,provid:[0,1,2,5,7],put:[1,7],pypi:5,python:[0,1,5,7],queri:[2,7],question:7,quot:7,rais:2,rang:1,rate:2,raw:1,read:[0,1,2,3,5,7],read_opt:2,reader:2,readi:7,recip:[2,4,5],recipe_fil:[0,2,7],recipes_dir:[0,7],recommend:7,ref:[],refer:[0,4,7],register_procedur:2,rel:[0,2,7],relat:[0,2,7],remov:1,renam:3,replac:[1,2],repo:[0,7],report:[5,7],repres:2,represent:2,republ:1,requir:[0,2,5,7],res:[0,7],reserv:7,reset_data:2,reslut:1,resourc:[1,3],result:[0,1,2],roll:[0,2,7],root:[1,2],routin:2,row:[0,2,3,5,7],row_filt:[0,2,7],run:[0,2,5],run_op:2,run_recip:[0,5,7],rval:1,same:7,save:[0,1,2,3,7],schema:[1,4],scope:2,script:[0,1,7],search:[2,4,7],section:1,see:[0,1,2,5,7],select:[0,2,7],self:2,semio:5,send:2,sep:1,seper:7,seri:1,serv:2,set:[0,1,2,5],setuptool:5,sever:7,shoul:7,should:[0,1,2,7],show:[2,5],shown:[0,7],shuld:2,sigfig:1,signific:1,simpl:2,simpli:[0,7],singl:1,size:[0,2,7],skip:[0,1,2,7],smooth:[0,1,7],some:[0,1,2,7],some_measure_concept:[0,7],someth:2,sometim:1,sourc:[1,5,7],spec:1,specif:[0,7],specifi:[0,7],speedup:3,split:[1,2,5,7],split_ent:2,split_kei:1,split_path:1,split_transl:5,split_translations_csv:1,split_translations_json:1,splite:1,stai:[0,2,7],stair:1,start:[2,7],statement:2,statist:5,step:[0,2,7],store:[0,1,2,7],str:[0,2,5,6,7],string:[0,1,2,5,7],structur:4,style:[0,2,7],sub:[0,2,7],submodul:6,subpackag:6,sum:[0,2,7],suppli:2,support:[0,2,5,7],suppos:[0,7],swe:2,symbol:[2,7],system:7,systemat:1,tabl:1,tabular:[1,5],tail:1,take:7,target:[0,2,7],target_column:[0,1,2,7],task:[1,2,5,7],tell:[0,7],templat:[2,7],test:2,text_befor:2,than:1,thei:[0,2,7],them:[0,1,2,7],thi:[0,1,2,5,7],thing:[3,7],though:[0,7],threshold:1,time:[0,1,2,7],to_concept_id:1,to_csv:1,to_ddfcsv:3,to_graph:2,to_recip:2,todo:[7,8],togeth:[0,7],too:7,tool:[5,7],top:[0,7],total:0,transform:[0,2,5,6,7],translat:[0,1,2,5,7],translate_column:[1,2],translate_head:[1,2],treat:0,tree:[0,2,7],tree_view:2,trend:[0,2,5],trend_bridg:[1,2],two:[0,2,7],type:[0,1,2,7],under:[2,7],underli:2,union:[0,1,2,7],unpop:7,updat:[1,5],upper:2,upper_case_nam:[0,7],upstream:2,upstream_list:2,upstream_node_id:2,url:1,usa:[2,7],usag:[0,4,7],use:[0,1,2,5,7],use_exist:1,used:[0,1,2,7],useful:[2,5],user:[1,2],using:[0,2,7],util:[0,2,5,7],val:[0,2,7],valid:[0,2,3,4,5],validate_recip:[5,7],valu:[0,1,2,7],variabl:[0,2,7],variou:5,version:[5,7],wai:[0,2,7],want:[0,1,2,7],warpper:2,well:7,what:[1,2,4],when:[0,1,2,5,7],where:[0,2,7],whether:1,which:[0,1,2,7],whole:[0,2,7],wildcard:[2,7],window:2,wip:[0,7],without:7,work:[1,5,7],workflow:1,write:[2,4,5],writer:7,written:7,wrong:7,yaml:[0,7],year:[0,1,2,7],yml:7,you:[0,1,5,7],your:4,your_compani:7,zcore:2,zero:1},titles:["Recipe Cookbook (draft)","ddf_utils package","ddf_utils.chef package","ddf_utils.model package","Welcome to ddf_utils\u2019s documentation!","Introduction","API References","Recipe Cookbook (draft)","Use ddf_utils for ETL tasks"],titleterms:{For:5,Use:8,add:7,api:[2,6],avail:[0,7],basic:7,check:7,chef:2,cli:1,command:5,config:[0,7],cook:[0,2,7],cookbook:[0,7],copi:0,dag:2,datapackag:1,ddf:3,ddf_util:[1,2,3,4,8],defin:7,dish:7,document:4,draft:[0,7],etl:8,execut:[0,7],extract_concept:[0,7],filter:7,filter_item:[0,7],filter_row:[0,7],first:[0,7],flatten:7,gener:[0,7],groupbi:[0,7],guidelin:[0,7],helper:[2,5],i18n:1,ident:0,includ:[0,7],indic:4,info:[0,7],ingredi:[0,2,7],instal:5,intermedi:7,introduct:5,librari:5,line:5,merg:[0,7],merge_ent:7,model:3,modul:[1,2,3],ops:2,option:7,packag:[1,2,3],patch:1,procedur:[0,2,7],prologu:7,recip:[0,7],refer:6,result:7,run:7,run_op:[0,7],schema:7,section:[0,7],serv:[0,7],set:7,split_ent:7,str:1,structur:[0,7],submodul:[1,2,3],subpackag:1,tabl:4,task:8,transform:1,translate_column:[0,7],translate_head:[0,7],trend_bridg:[0,7],usag:5,user:5,valid:7,welcom:4,what:[0,7],window:[0,5,7],write:[0,7],your:[0,7]}})
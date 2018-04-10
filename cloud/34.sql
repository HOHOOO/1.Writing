CREATE TABLE FEATURE_CLICK_DRAFT1 AS SELECT user_proxy_key,tag_id as key,user_tag_weight AS value from recommend.DW_CP_USER_PREFERENCE_LONG_TERM where tag_id regexp 'cate_' and tag_id in ('cate_17','cate_71','cate_87','cate_103','cate_107','cate_125','cate_207','cate_211','cate_213','cate_215','cate_217','cate_219','cate_223','cate_227','cate_229','cate_231','cate_233','cate_235','cate_237','cate_239','cate_241','cate_243','cate_245','cate_253','cate_255','cate_257','cate_261','cate_263','cate_265','cate_271','cate_283','cate_289','cate_291','cate_293','cate_295','cate_297','cate_299','cate_301','cate_303','cate_305','cate_307','cate_309','cate_311','cate_313','cate_315','cate_325','cate_329','cate_333','cate_339','cate_341','cate_345','cate_351','cate_353','cate_355','cate_359','cate_365','cate_385','cate_387','cate_389','cate_391','cate_393','cate_397','cate_399','cate_401','cate_403','cate_407','cate_413','cate_417','cate_421','cate_423','cate_425','cate_433','cate_435','cate_437','cate_445','cate_447','cate_451','cate_453','cate_455','cate_457','cate_461','cate_463','cate_465','cate_467','cate_469','cate_471','cate_473','cate_475','cate_477','cate_481','cate_487','cate_489','cate_491','cate_493','cate_497','cate_499','cate_503','cate_505','cate_509','cate_511','cate_519','cate_521','cate_523','cate_525','cate_527','cate_541','cate_543','cate_545','cate_551','cate_567','cate_569','cate_571','cate_573','cate_575','cate_577','cate_579','cate_581','cate_585','cate_593','cate_597','cate_601','cate_603','cate_605','cate_611','cate_613','cate_617','cate_619','cate_623','cate_625','cate_659','cate_661','cate_663','cate_665','cate_667','cate_669','cate_671','cate_673','cate_675','cate_677','cate_679','cate_681','cate_683','cate_685','cate_687','cate_691','cate_693','cate_695','cate_697','cate_699','cate_705','cate_707','cate_711','cate_723','cate_725','cate_727','cate_737','cate_739','cate_741','cate_743','cate_747','cate_749','cate_751','cate_753','cate_755','cate_757','cate_767','cate_777','cate_803','cate_805','cate_807','cate_809','cate_811','cate_827','cate_853','cate_855','cate_857','cate_859','cate_871','cate_875','cate_877','cate_879','cate_881','cate_883','cate_885','cate_889','cate_903','cate_909','cate_911','cate_913','cate_915','cate_917','cate_921','cate_923','cate_925','cate_927','cate_929','cate_931','cate_933','cate_935','cate_955','cate_959','cate_961','cate_963','cate_965','cate_967','cate_969','cate_971','cate_975','cate_987','cate_989','cate_991','cate_993','cate_1009','cate_1013','cate_1047','cate_1049','cate_1055','cate_1057','cate_1059','cate_1067','cate_1069','cate_1071','cate_1077','cate_1079','cate_1081','cate_1085','cate_1087','cate_1091','cate_1093','cate_1095','cate_1097','cate_1099','cate_1111','cate_1117','cate_1119','cate_1125','cate_1139','cate_1141','cate_1143','cate_1145','cate_1147','cate_1149','cate_1151','cate_1153','cate_1155','cate_1157','cate_1159','cate_1161','cate_1163','cate_1173','cate_1177','cate_1179','cate_1181','cate_1185','cate_1187','cate_1207','cate_1209','cate_1211','cate_1213','cate_1217','cate_1219','cate_1225','cate_1243','cate_1245','cate_1247','cate_1251','cate_1253','cate_1255','cate_1257','cate_1259','cate_1261','cate_1263','cate_1265','cate_1269','cate_1275','cate_1277','cate_1279','cate_1283','cate_1285','cate_1287','cate_1289','cate_1291','cate_1293','cate_1295','cate_1297','cate_1299','cate_1301','cate_1303','cate_1305','cate_1307','cate_1309','cate_1311','cate_1315','cate_1321','cate_1323','cate_1325','cate_1327','cate_1331','cate_1333','cate_1335','cate_1347','cate_1351','cate_1353','cate_1365','cate_1387','cate_1425','cate_1427','cate_1429','cate_1437','cate_1439','cate_1453','cate_1455','cate_1471','cate_1477','cate_1481','cate_1489','cate_1491','cate_1495','cate_1505','cate_1529','cate_1531','cate_1533','cate_1537','cate_1539','cate_1553','cate_1557','cate_1559','cate_1561','cate_1567','cate_1569','cate_1571','cate_1573','cate_1575','cate_1577','cate_1579','cate_1581','cate_1583','cate_1585','cate_1587','cate_1589','cate_1591','cate_1593','cate_1597','cate_1599','cate_1601','cate_1603','cate_1605','cate_1607','cate_1609','cate_1611','cate_1617','cate_1619','cate_1627','cate_1629','cate_1631','cate_1633','cate_1639','cate_1647','cate_1649','cate_1665','cate_1689','cate_1695','cate_1705','cate_1709','cate_1711','cate_1713','cate_1717','cate_1729','cate_1733','cate_1747','cate_1749','cate_1831','cate_1967','cate_2069','cate_2071','cate_2073','cate_2075','cate_2191','cate_2247','cate_2251','cate_2321','cate_2323','cate_2325','cate_2327','cate_2381','cate_2407','cate_2411','cate_2413','cate_2419','cate_2421','cate_2423','cate_2447','cate_2449','cate_2451','cate_2453','cate_2455','cate_2457','cate_2459','cate_2461','cate_2463','cate_2465','cate_2467','cate_2587','cate_2589','cate_2591','cate_2593','cate_2595','cate_2597','cate_2599','cate_2601','cate_2605','cate_2625','cate_2627','cate_2629','cate_2631','cate_2651','cate_2657','cate_2715','cate_2723','cate_2725','cate_2727','cate_2731','cate_2741','cate_2743','cate_2745','cate_2749','cate_2769','cate_2775','cate_2777','cate_2779','cate_2781','cate_2793','cate_2795','cate_2797','cate_2803','cate_2805','cate_2807','cate_2809','cate_2811','cate_2813','cate_2815','cate_2817','cate_2819','cate_2835','cate_2859','cate_2863','cate_2911','cate_2965','cate_2967','cate_3005','cate_3015','cate_3021','cate_3025','cate_3029','cate_3099','cate_3147','cate_3159','cate_3195','cate_3203','cate_3209','cate_3213','cate_3215','cate_3217','cate_3255','cate_3333','cate_3365','cate_3367','cate_3381','cate_3391','cate_3401','cate_3407','cate_3409','cate_3429','cate_3465','cate_3483','cate_3495','cate_3503','cate_3583','cate_3601','cate_3633','cate_3639','cate_3641','cate_3643','cate_3645','cate_3647','cate_3649','cate_3651','cate_3653','cate_3655','cate_3657','cate_3663','cate_3677','cate_3697','cate_3709','cate_3711','cate_3745','cate_3787','cate_3805','cate_3851','cate_3879','cate_3907','cate_3911','cate_3921','cate_3933','cate_3941','cate_3945','cate_3949','cate_3951','cate_3955','cate_3957','cate_3959','cate_3961','cate_3965','cate_4031','cate_4035','cate_4037','cate_4039','cate_4041','cate_4043','cate_4045','cate_4047','cate_4049','cate_4051','cate_4055','cate_4057','cate_4097','cate_4175','cate_4181','cate_4185','cate_4189','cate_4195','cate_4197','cate_4213','cate_4223','cate_4239','cate_4247','cate_4257','cate_4259','cate_4283','cate_4289','cate_4375','cate_4379','cate_4383','cate_4385','cate_4389','cate_4393','cate_4395','cate_4399','cate_4403','cate_4409','cate_4413','cate_4417','cate_4419','cate_4421','cate_4423','cate_4425','cate_4429','cate_4551','cate_4625','cate_4635','cate_4655','cate_4659','cate_4713','cate_4715','cate_4717','cate_4719','cate_4721','cate_4729','cate_4735','cate_4741','cate_4777','cate_4779','cate_4781','cate_4783','cate_4785','cate_4787','cate_4807','cate_4813','cate_4815','cate_4817','cate_4819','cate_4821','cate_4847','cate_4851','cate_4865','cate_4875','cate_4877','cate_4879','cate_4881','cate_4889','cate_4891','cate_4893','cate_4895','cate_4911','cate_4913','cate_4915','cate_4917','cate_4919','cate_4921','cate_4941','cate_4943','cate_4967','cate_4973','cate_4983','cate_4993','cate_5001','cate_5025','cate_5041','cate_5065','cate_5073','cate_5081','cate_5099','cate_5121','cate_5143','cate_5151','cate_5157','cate_5165','cate_5167','cate_5173','cate_5175','cate_5181','cate_5185','cate_5187','cate_5189','cate_5191','cate_5195','cate_5197','cate_5199','cate_5201','cate_5203','cate_5205','cate_5209','cate_5223','cate_5233','cate_5235','cate_5245','cate_5251','cate_5259','cate_5271','cate_5273','cate_5275','cate_5277','cate_5285','cate_5295','cate_5299','cate_5301','cate_5303','cate_5305','cate_5307','cate_5309','cate_5323','cate_5324','cate_5327','cate_5333','cate_5334','cate_5336','cate_5341','cate_5342','cate_5343','cate_5344','cate_5353','cate_5354','cate_5355','cate_5356','cate_5367','cate_5368','cate_5369','cate_5370','cate_5371','cate_5376','cate_5377','cate_5378','cate_5379','cate_5380','cate_5381','cate_5390','cate_5391','cate_5392','cate_5393','cate_5394','cate_5395','cate_5396','cate_5403','cate_5404','cate_5406','cate_5407','cate_5414','cate_5415','cate_5416','cate_5417','cate_5418','cate_5424','cate_5425','cate_5426','cate_5427','cate_5428','cate_5429','cate_5430','cate_5431','cate_5432','cate_5433','cate_5434','cate_5435','cate_5436','cate_5437','cate_5438','cate_5439','cate_5440','cate_5441','cate_5442','cate_5443','cate_5444','cate_5445','cate_5446','cate_5462','cate_5463','cate_5464','cate_5465','cate_5466','cate_5468','cate_5469','cate_5470','cate_5473','cate_5474','cate_5475','cate_5476','cate_5477','cate_5482','cate_5510','cate_5511','cate_5527','cate_5528','cate_5532','cate_5539') GROUP BY user_proxy_key;

CREATE TABLE FEATURE_CLICK_DRAFT2 AS SELECT
user_proxy_key,
kv['cate_17'] as cate_17,kv['cate_71'] as cate_71,kv['cate_87'] as cate_87,kv['cate_103'] as cate_103,kv['cate_107'] as cate_107,kv['cate_125'] as cate_125,kv['cate_207'] as cate_207,kv['cate_211'] as cate_211,kv['cate_213'] as cate_213,kv['cate_215'] as cate_215,kv['cate_217'] as cate_217,kv['cate_219'] as cate_219,kv['cate_223'] as cate_223,kv['cate_227'] as cate_227,kv['cate_229'] as cate_229,kv['cate_231'] as cate_231,kv['cate_233'] as cate_233,kv['cate_235'] as cate_235,kv['cate_237'] as cate_237,kv['cate_239'] as cate_239,kv['cate_241'] as cate_241,kv['cate_243'] as cate_243,kv['cate_245'] as cate_245,kv['cate_253'] as cate_253,kv['cate_255'] as cate_255,kv['cate_257'] as cate_257,kv['cate_261'] as cate_261,kv['cate_263'] as cate_263,kv['cate_265'] as cate_265,kv['cate_271'] as cate_271,kv['cate_283'] as cate_283,kv['cate_289'] as cate_289,kv['cate_291'] as cate_291,kv['cate_293'] as cate_293,kv['cate_295'] as cate_295,kv['cate_297'] as cate_297,kv['cate_299'] as cate_299,kv['cate_301'] as cate_301,kv['cate_303'] as cate_303,kv['cate_305'] as cate_305,kv['cate_307'] as cate_307,kv['cate_309'] as cate_309,kv['cate_311'] as cate_311,kv['cate_313'] as cate_313,kv['cate_315'] as cate_315,kv['cate_325'] as cate_325,kv['cate_329'] as cate_329,kv['cate_333'] as cate_333,kv['cate_339'] as cate_339,kv['cate_341'] as cate_341,kv['cate_345'] as cate_345,kv['cate_351'] as cate_351,kv['cate_353'] as cate_353,kv['cate_355'] as cate_355,kv['cate_359'] as cate_359,kv['cate_365'] as cate_365,kv['cate_385'] as cate_385,kv['cate_387'] as cate_387,kv['cate_389'] as cate_389,kv['cate_391'] as cate_391,kv['cate_393'] as cate_393,kv['cate_397'] as cate_397,kv['cate_399'] as cate_399,kv['cate_401'] as cate_401,kv['cate_403'] as cate_403,kv['cate_407'] as cate_407,kv['cate_413'] as cate_413,kv['cate_417'] as cate_417,kv['cate_421'] as cate_421,kv['cate_423'] as cate_423,kv['cate_425'] as cate_425,kv['cate_433'] as cate_433,kv['cate_435'] as cate_435,kv['cate_437'] as cate_437,kv['cate_445'] as cate_445,kv['cate_447'] as cate_447,kv['cate_451'] as cate_451,kv['cate_453'] as cate_453,kv['cate_455'] as cate_455,kv['cate_457'] as cate_457,kv['cate_461'] as cate_461,kv['cate_463'] as cate_463,kv['cate_465'] as cate_465,kv['cate_467'] as cate_467,kv['cate_469'] as cate_469,kv['cate_471'] as cate_471,kv['cate_473'] as cate_473,kv['cate_475'] as cate_475,kv['cate_477'] as cate_477,kv['cate_481'] as cate_481,kv['cate_487'] as cate_487,kv['cate_489'] as cate_489,kv['cate_491'] as cate_491,kv['cate_493'] as cate_493,kv['cate_497'] as cate_497,kv['cate_499'] as cate_499,kv['cate_503'] as cate_503,kv['cate_505'] as cate_505,kv['cate_509'] as cate_509,kv['cate_511'] as cate_511,kv['cate_519'] as cate_519,kv['cate_521'] as cate_521,kv['cate_523'] as cate_523,kv['cate_525'] as cate_525,kv['cate_527'] as cate_527,kv['cate_541'] as cate_541,kv['cate_543'] as cate_543,kv['cate_545'] as cate_545,kv['cate_551'] as cate_551,kv['cate_567'] as cate_567,kv['cate_569'] as cate_569,kv['cate_571'] as cate_571,kv['cate_573'] as cate_573,kv['cate_575'] as cate_575,kv['cate_577'] as cate_577,kv['cate_579'] as cate_579,kv['cate_581'] as cate_581,kv['cate_585'] as cate_585,kv['cate_593'] as cate_593,kv['cate_597'] as cate_597,kv['cate_601'] as cate_601,kv['cate_603'] as cate_603,kv['cate_605'] as cate_605,kv['cate_611'] as cate_611,kv['cate_613'] as cate_613,kv['cate_617'] as cate_617,kv['cate_619'] as cate_619,kv['cate_623'] as cate_623,kv['cate_625'] as cate_625,kv['cate_659'] as cate_659,kv['cate_661'] as cate_661,kv['cate_663'] as cate_663,kv['cate_665'] as cate_665,kv['cate_667'] as cate_667,kv['cate_669'] as cate_669,kv['cate_671'] as cate_671,kv['cate_673'] as cate_673,kv['cate_675'] as cate_675,kv['cate_677'] as cate_677,kv['cate_679'] as cate_679,kv['cate_681'] as cate_681,kv['cate_683'] as cate_683,kv['cate_685'] as cate_685,kv['cate_687'] as cate_687,kv['cate_691'] as cate_691,kv['cate_693'] as cate_693,kv['cate_695'] as cate_695,kv['cate_697'] as cate_697,kv['cate_699'] as cate_699,kv['cate_705'] as cate_705,kv['cate_707'] as cate_707,kv['cate_711'] as cate_711,kv['cate_723'] as cate_723,kv['cate_725'] as cate_725,kv['cate_727'] as cate_727,kv['cate_737'] as cate_737,kv['cate_739'] as cate_739,kv['cate_741'] as cate_741,kv['cate_743'] as cate_743,kv['cate_747'] as cate_747,kv['cate_749'] as cate_749,kv['cate_751'] as cate_751,kv['cate_753'] as cate_753,kv['cate_755'] as cate_755,kv['cate_757'] as cate_757,kv['cate_767'] as cate_767,kv['cate_777'] as cate_777,kv['cate_803'] as cate_803,kv['cate_805'] as cate_805,kv['cate_807'] as cate_807,kv['cate_809'] as cate_809,kv['cate_811'] as cate_811,kv['cate_827'] as cate_827,kv['cate_853'] as cate_853,kv['cate_855'] as cate_855,kv['cate_857'] as cate_857,kv['cate_859'] as cate_859,kv['cate_871'] as cate_871,kv['cate_875'] as cate_875,kv['cate_877'] as cate_877,kv['cate_879'] as cate_879,kv['cate_881'] as cate_881,kv['cate_883'] as cate_883,kv['cate_885'] as cate_885,kv['cate_889'] as cate_889,kv['cate_903'] as cate_903,kv['cate_909'] as cate_909,kv['cate_911'] as cate_911,kv['cate_913'] as cate_913,kv['cate_915'] as cate_915,kv['cate_917'] as cate_917,kv['cate_921'] as cate_921,kv['cate_923'] as cate_923,kv['cate_925'] as cate_925,kv['cate_927'] as cate_927,kv['cate_929'] as cate_929,kv['cate_931'] as cate_931,kv['cate_933'] as cate_933,kv['cate_935'] as cate_935,kv['cate_955'] as cate_955,kv['cate_959'] as cate_959,kv['cate_961'] as cate_961,kv['cate_963'] as cate_963,kv['cate_965'] as cate_965,kv['cate_967'] as cate_967,kv['cate_969'] as cate_969,kv['cate_971'] as cate_971,kv['cate_975'] as cate_975,kv['cate_987'] as cate_987,kv['cate_989'] as cate_989,kv['cate_991'] as cate_991,kv['cate_993'] as cate_993,kv['cate_1009'] as cate_1009,kv['cate_1013'] as cate_1013,kv['cate_1047'] as cate_1047,kv['cate_1049'] as cate_1049,kv['cate_1055'] as cate_1055,kv['cate_1057'] as cate_1057,kv['cate_1059'] as cate_1059,kv['cate_1067'] as cate_1067,kv['cate_1069'] as cate_1069,kv['cate_1071'] as cate_1071,kv['cate_1077'] as cate_1077,kv['cate_1079'] as cate_1079,kv['cate_1081'] as cate_1081,kv['cate_1085'] as cate_1085,kv['cate_1087'] as cate_1087,kv['cate_1091'] as cate_1091,kv['cate_1093'] as cate_1093,kv['cate_1095'] as cate_1095,kv['cate_1097'] as cate_1097,kv['cate_1099'] as cate_1099,kv['cate_1111'] as cate_1111,kv['cate_1117'] as cate_1117,kv['cate_1119'] as cate_1119,kv['cate_1125'] as cate_1125,kv['cate_1139'] as cate_1139,kv['cate_1141'] as cate_1141,kv['cate_1143'] as cate_1143,kv['cate_1145'] as cate_1145,kv['cate_1147'] as cate_1147,kv['cate_1149'] as cate_1149,kv['cate_1151'] as cate_1151,kv['cate_1153'] as cate_1153,kv['cate_1155'] as cate_1155,kv['cate_1157'] as cate_1157,kv['cate_1159'] as cate_1159,kv['cate_1161'] as cate_1161,kv['cate_1163'] as cate_1163,kv['cate_1173'] as cate_1173,kv['cate_1177'] as cate_1177,kv['cate_1179'] as cate_1179,kv['cate_1181'] as cate_1181,kv['cate_1185'] as cate_1185,kv['cate_1187'] as cate_1187,kv['cate_1207'] as cate_1207,kv['cate_1209'] as cate_1209,kv['cate_1211'] as cate_1211,kv['cate_1213'] as cate_1213,kv['cate_1217'] as cate_1217,kv['cate_1219'] as cate_1219,kv['cate_1225'] as cate_1225,kv['cate_1243'] as cate_1243,kv['cate_1245'] as cate_1245,kv['cate_1247'] as cate_1247,kv['cate_1251'] as cate_1251,kv['cate_1253'] as cate_1253,kv['cate_1255'] as cate_1255,kv['cate_1257'] as cate_1257,kv['cate_1259'] as cate_1259,kv['cate_1261'] as cate_1261,kv['cate_1263'] as cate_1263,kv['cate_1265'] as cate_1265,kv['cate_1269'] as cate_1269,kv['cate_1275'] as cate_1275,kv['cate_1277'] as cate_1277,kv['cate_1279'] as cate_1279,kv['cate_1283'] as cate_1283,kv['cate_1285'] as cate_1285,kv['cate_1287'] as cate_1287,kv['cate_1289'] as cate_1289,kv['cate_1291'] as cate_1291,kv['cate_1293'] as cate_1293,kv['cate_1295'] as cate_1295,kv['cate_1297'] as cate_1297,kv['cate_1299'] as cate_1299,kv['cate_1301'] as cate_1301,kv['cate_1303'] as cate_1303,kv['cate_1305'] as cate_1305,kv['cate_1307'] as cate_1307,kv['cate_1309'] as cate_1309,kv['cate_1311'] as cate_1311,kv['cate_1315'] as cate_1315,kv['cate_1321'] as cate_1321,kv['cate_1323'] as cate_1323,kv['cate_1325'] as cate_1325,kv['cate_1327'] as cate_1327,kv['cate_1331'] as cate_1331,kv['cate_1333'] as cate_1333,kv['cate_1335'] as cate_1335,kv['cate_1347'] as cate_1347,kv['cate_1351'] as cate_1351,kv['cate_1353'] as cate_1353,kv['cate_1365'] as cate_1365,kv['cate_1387'] as cate_1387,kv['cate_1425'] as cate_1425,kv['cate_1427'] as cate_1427,kv['cate_1429'] as cate_1429,kv['cate_1437'] as cate_1437,kv['cate_1439'] as cate_1439,kv['cate_1453'] as cate_1453,kv['cate_1455'] as cate_1455,kv['cate_1471'] as cate_1471,kv['cate_1477'] as cate_1477,kv['cate_1481'] as cate_1481,kv['cate_1489'] as cate_1489,kv['cate_1491'] as cate_1491,kv['cate_1495'] as cate_1495,kv['cate_1505'] as cate_1505,kv['cate_1529'] as cate_1529,kv['cate_1531'] as cate_1531,kv['cate_1533'] as cate_1533,kv['cate_1537'] as cate_1537,kv['cate_1539'] as cate_1539,kv['cate_1553'] as cate_1553,kv['cate_1557'] as cate_1557,kv['cate_1559'] as cate_1559,kv['cate_1561'] as cate_1561,kv['cate_1567'] as cate_1567,kv['cate_1569'] as cate_1569,kv['cate_1571'] as cate_1571,kv['cate_1573'] as cate_1573,kv['cate_1575'] as cate_1575,kv['cate_1577'] as cate_1577,kv['cate_1579'] as cate_1579,kv['cate_1581'] as cate_1581,kv['cate_1583'] as cate_1583,kv['cate_1585'] as cate_1585,kv['cate_1587'] as cate_1587,kv['cate_1589'] as cate_1589,kv['cate_1591'] as cate_1591,kv['cate_1593'] as cate_1593,kv['cate_1597'] as cate_1597,kv['cate_1599'] as cate_1599,kv['cate_1601'] as cate_1601,kv['cate_1603'] as cate_1603,kv['cate_1605'] as cate_1605,kv['cate_1607'] as cate_1607,kv['cate_1609'] as cate_1609,kv['cate_1611'] as cate_1611,kv['cate_1617'] as cate_1617,kv['cate_1619'] as cate_1619,kv['cate_1627'] as cate_1627,kv['cate_1629'] as cate_1629,kv['cate_1631'] as cate_1631,kv['cate_1633'] as cate_1633,kv['cate_1639'] as cate_1639,kv['cate_1647'] as cate_1647,kv['cate_1649'] as cate_1649,kv['cate_1665'] as cate_1665,kv['cate_1689'] as cate_1689,kv['cate_1695'] as cate_1695,kv['cate_1705'] as cate_1705,kv['cate_1709'] as cate_1709,kv['cate_1711'] as cate_1711,kv['cate_1713'] as cate_1713,kv['cate_1717'] as cate_1717,kv['cate_1729'] as cate_1729,kv['cate_1733'] as cate_1733,kv['cate_1747'] as cate_1747,kv['cate_1749'] as cate_1749,kv['cate_1831'] as cate_1831,kv['cate_1967'] as cate_1967,kv['cate_2069'] as cate_2069,kv['cate_2071'] as cate_2071,kv['cate_2073'] as cate_2073,kv['cate_2075'] as cate_2075,kv['cate_2191'] as cate_2191,kv['cate_2247'] as cate_2247,kv['cate_2251'] as cate_2251,kv['cate_2321'] as cate_2321,kv['cate_2323'] as cate_2323,kv['cate_2325'] as cate_2325,kv['cate_2327'] as cate_2327,kv['cate_2381'] as cate_2381,kv['cate_2407'] as cate_2407,kv['cate_2411'] as cate_2411,kv['cate_2413'] as cate_2413,kv['cate_2419'] as cate_2419,kv['cate_2421'] as cate_2421,kv['cate_2423'] as cate_2423,kv['cate_2447'] as cate_2447,kv['cate_2449'] as cate_2449,kv['cate_2451'] as cate_2451,kv['cate_2453'] as cate_2453,kv['cate_2455'] as cate_2455,kv['cate_2457'] as cate_2457,kv['cate_2459'] as cate_2459,kv['cate_2461'] as cate_2461,kv['cate_2463'] as cate_2463,kv['cate_2465'] as cate_2465,kv['cate_2467'] as cate_2467,kv['cate_2587'] as cate_2587,kv['cate_2589'] as cate_2589,kv['cate_2591'] as cate_2591,kv['cate_2593'] as cate_2593,kv['cate_2595'] as cate_2595,kv['cate_2597'] as cate_2597,kv['cate_2599'] as cate_2599,kv['cate_2601'] as cate_2601,kv['cate_2605'] as cate_2605,kv['cate_2625'] as cate_2625,kv['cate_2627'] as cate_2627,kv['cate_2629'] as cate_2629,kv['cate_2631'] as cate_2631,kv['cate_2651'] as cate_2651,kv['cate_2657'] as cate_2657,kv['cate_2715'] as cate_2715,kv['cate_2723'] as cate_2723,kv['cate_2725'] as cate_2725,kv['cate_2727'] as cate_2727,kv['cate_2731'] as cate_2731,kv['cate_2741'] as cate_2741,kv['cate_2743'] as cate_2743,kv['cate_2745'] as cate_2745,kv['cate_2749'] as cate_2749,kv['cate_2769'] as cate_2769,kv['cate_2775'] as cate_2775,kv['cate_2777'] as cate_2777,kv['cate_2779'] as cate_2779,kv['cate_2781'] as cate_2781,kv['cate_2793'] as cate_2793,kv['cate_2795'] as cate_2795,kv['cate_2797'] as cate_2797,kv['cate_2803'] as cate_2803,kv['cate_2805'] as cate_2805,kv['cate_2807'] as cate_2807,kv['cate_2809'] as cate_2809,kv['cate_2811'] as cate_2811,kv['cate_2813'] as cate_2813,kv['cate_2815'] as cate_2815,kv['cate_2817'] as cate_2817,kv['cate_2819'] as cate_2819,kv['cate_2835'] as cate_2835,kv['cate_2859'] as cate_2859,kv['cate_2863'] as cate_2863,kv['cate_2911'] as cate_2911,kv['cate_2965'] as cate_2965,kv['cate_2967'] as cate_2967,kv['cate_3005'] as cate_3005,kv['cate_3015'] as cate_3015,kv['cate_3021'] as cate_3021,kv['cate_3025'] as cate_3025,kv['cate_3029'] as cate_3029,kv['cate_3099'] as cate_3099,kv['cate_3147'] as cate_3147,kv['cate_3159'] as cate_3159,kv['cate_3195'] as cate_3195,kv['cate_3203'] as cate_3203,kv['cate_3209'] as cate_3209,kv['cate_3213'] as cate_3213,kv['cate_3215'] as cate_3215,kv['cate_3217'] as cate_3217,kv['cate_3255'] as cate_3255,kv['cate_3333'] as cate_3333,kv['cate_3365'] as cate_3365,kv['cate_3367'] as cate_3367,kv['cate_3381'] as cate_3381,kv['cate_3391'] as cate_3391,kv['cate_3401'] as cate_3401,kv['cate_3407'] as cate_3407,kv['cate_3409'] as cate_3409,kv['cate_3429'] as cate_3429,kv['cate_3465'] as cate_3465,kv['cate_3483'] as cate_3483,kv['cate_3495'] as cate_3495,kv['cate_3503'] as cate_3503,kv['cate_3583'] as cate_3583,kv['cate_3601'] as cate_3601,kv['cate_3633'] as cate_3633,kv['cate_3639'] as cate_3639,kv['cate_3641'] as cate_3641,kv['cate_3643'] as cate_3643,kv['cate_3645'] as cate_3645,kv['cate_3647'] as cate_3647,kv['cate_3649'] as cate_3649,kv['cate_3651'] as cate_3651,kv['cate_3653'] as cate_3653,kv['cate_3655'] as cate_3655,kv['cate_3657'] as cate_3657,kv['cate_3663'] as cate_3663,kv['cate_3677'] as cate_3677,kv['cate_3697'] as cate_3697,kv['cate_3709'] as cate_3709,kv['cate_3711'] as cate_3711,kv['cate_3745'] as cate_3745,kv['cate_3787'] as cate_3787,kv['cate_3805'] as cate_3805,kv['cate_3851'] as cate_3851,kv['cate_3879'] as cate_3879,kv['cate_3907'] as cate_3907,kv['cate_3911'] as cate_3911,kv['cate_3921'] as cate_3921,kv['cate_3933'] as cate_3933,kv['cate_3941'] as cate_3941,kv['cate_3945'] as cate_3945,kv['cate_3949'] as cate_3949,kv['cate_3951'] as cate_3951,kv['cate_3955'] as cate_3955,kv['cate_3957'] as cate_3957,kv['cate_3959'] as cate_3959,kv['cate_3961'] as cate_3961,kv['cate_3965'] as cate_3965,kv['cate_4031'] as cate_4031,kv['cate_4035'] as cate_4035,kv['cate_4037'] as cate_4037,kv['cate_4039'] as cate_4039,kv['cate_4041'] as cate_4041,kv['cate_4043'] as cate_4043,kv['cate_4045'] as cate_4045,kv['cate_4047'] as cate_4047,kv['cate_4049'] as cate_4049,kv['cate_4051'] as cate_4051,kv['cate_4055'] as cate_4055,kv['cate_4057'] as cate_4057,kv['cate_4097'] as cate_4097,kv['cate_4175'] as cate_4175,kv['cate_4181'] as cate_4181,kv['cate_4185'] as cate_4185,kv['cate_4189'] as cate_4189,kv['cate_4195'] as cate_4195,kv['cate_4197'] as cate_4197,kv['cate_4213'] as cate_4213,kv['cate_4223'] as cate_4223,kv['cate_4239'] as cate_4239,kv['cate_4247'] as cate_4247,kv['cate_4257'] as cate_4257,kv['cate_4259'] as cate_4259,kv['cate_4283'] as cate_4283,kv['cate_4289'] as cate_4289,kv['cate_4375'] as cate_4375,kv['cate_4379'] as cate_4379,kv['cate_4383'] as cate_4383,kv['cate_4385'] as cate_4385,kv['cate_4389'] as cate_4389,kv['cate_4393'] as cate_4393,kv['cate_4395'] as cate_4395,kv['cate_4399'] as cate_4399,kv['cate_4403'] as cate_4403,kv['cate_4409'] as cate_4409,kv['cate_4413'] as cate_4413,kv['cate_4417'] as cate_4417,kv['cate_4419'] as cate_4419,kv['cate_4421'] as cate_4421,kv['cate_4423'] as cate_4423,kv['cate_4425'] as cate_4425,kv['cate_4429'] as cate_4429,kv['cate_4551'] as cate_4551,kv['cate_4625'] as cate_4625,kv['cate_4635'] as cate_4635,kv['cate_4655'] as cate_4655,kv['cate_4659'] as cate_4659,kv['cate_4713'] as cate_4713,kv['cate_4715'] as cate_4715,kv['cate_4717'] as cate_4717,kv['cate_4719'] as cate_4719,kv['cate_4721'] as cate_4721,kv['cate_4729'] as cate_4729,kv['cate_4735'] as cate_4735,kv['cate_4741'] as cate_4741,kv['cate_4777'] as cate_4777,kv['cate_4779'] as cate_4779,kv['cate_4781'] as cate_4781,kv['cate_4783'] as cate_4783,kv['cate_4785'] as cate_4785,kv['cate_4787'] as cate_4787,kv['cate_4807'] as cate_4807,kv['cate_4813'] as cate_4813,kv['cate_4815'] as cate_4815,kv['cate_4817'] as cate_4817,kv['cate_4819'] as cate_4819,kv['cate_4821'] as cate_4821,kv['cate_4847'] as cate_4847,kv['cate_4851'] as cate_4851,kv['cate_4865'] as cate_4865,kv['cate_4875'] as cate_4875,kv['cate_4877'] as cate_4877,kv['cate_4879'] as cate_4879,kv['cate_4881'] as cate_4881,kv['cate_4889'] as cate_4889,kv['cate_4891'] as cate_4891,kv['cate_4893'] as cate_4893,kv['cate_4895'] as cate_4895,kv['cate_4911'] as cate_4911,kv['cate_4913'] as cate_4913,kv['cate_4915'] as cate_4915,kv['cate_4917'] as cate_4917,kv['cate_4919'] as cate_4919,kv['cate_4921'] as cate_4921,kv['cate_4941'] as cate_4941,kv['cate_4943'] as cate_4943,kv['cate_4967'] as cate_4967,kv['cate_4973'] as cate_4973,kv['cate_4983'] as cate_4983,kv['cate_4993'] as cate_4993,kv['cate_5001'] as cate_5001,kv['cate_5025'] as cate_5025,kv['cate_5041'] as cate_5041,kv['cate_5065'] as cate_5065,kv['cate_5073'] as cate_5073,kv['cate_5081'] as cate_5081,kv['cate_5099'] as cate_5099,kv['cate_5121'] as cate_5121,kv['cate_5143'] as cate_5143,kv['cate_5151'] as cate_5151,kv['cate_5157'] as cate_5157,kv['cate_5165'] as cate_5165,kv['cate_5167'] as cate_5167,kv['cate_5173'] as cate_5173,kv['cate_5175'] as cate_5175,kv['cate_5181'] as cate_5181,kv['cate_5185'] as cate_5185,kv['cate_5187'] as cate_5187,kv['cate_5189'] as cate_5189,kv['cate_5191'] as cate_5191,kv['cate_5195'] as cate_5195,kv['cate_5197'] as cate_5197,kv['cate_5199'] as cate_5199,kv['cate_5201'] as cate_5201,kv['cate_5203'] as cate_5203,kv['cate_5205'] as cate_5205,kv['cate_5209'] as cate_5209,kv['cate_5223'] as cate_5223,kv['cate_5233'] as cate_5233,kv['cate_5235'] as cate_5235,kv['cate_5245'] as cate_5245,kv['cate_5251'] as cate_5251,kv['cate_5259'] as cate_5259,kv['cate_5271'] as cate_5271,kv['cate_5273'] as cate_5273,kv['cate_5275'] as cate_5275,kv['cate_5277'] as cate_5277,kv['cate_5285'] as cate_5285,kv['cate_5295'] as cate_5295,kv['cate_5299'] as cate_5299,kv['cate_5301'] as cate_5301,kv['cate_5303'] as cate_5303,kv['cate_5305'] as cate_5305,kv['cate_5307'] as cate_5307,kv['cate_5309'] as cate_5309,kv['cate_5323'] as cate_5323,kv['cate_5324'] as cate_5324,kv['cate_5327'] as cate_5327,kv['cate_5333'] as cate_5333,kv['cate_5334'] as cate_5334,kv['cate_5336'] as cate_5336,kv['cate_5341'] as cate_5341,kv['cate_5342'] as cate_5342,kv['cate_5343'] as cate_5343,kv['cate_5344'] as cate_5344,kv['cate_5353'] as cate_5353,kv['cate_5354'] as cate_5354,kv['cate_5355'] as cate_5355,kv['cate_5356'] as cate_5356,kv['cate_5367'] as cate_5367,kv['cate_5368'] as cate_5368,kv['cate_5369'] as cate_5369,kv['cate_5370'] as cate_5370,kv['cate_5371'] as cate_5371,kv['cate_5376'] as cate_5376,kv['cate_5377'] as cate_5377,kv['cate_5378'] as cate_5378,kv['cate_5379'] as cate_5379,kv['cate_5380'] as cate_5380,kv['cate_5381'] as cate_5381,kv['cate_5390'] as cate_5390,kv['cate_5391'] as cate_5391,kv['cate_5392'] as cate_5392,kv['cate_5393'] as cate_5393,kv['cate_5394'] as cate_5394,kv['cate_5395'] as cate_5395,kv['cate_5396'] as cate_5396,kv['cate_5403'] as cate_5403,kv['cate_5404'] as cate_5404,kv['cate_5406'] as cate_5406,kv['cate_5407'] as cate_5407,kv['cate_5414'] as cate_5414,kv['cate_5415'] as cate_5415,kv['cate_5416'] as cate_5416,kv['cate_5417'] as cate_5417,kv['cate_5418'] as cate_5418,kv['cate_5424'] as cate_5424,kv['cate_5425'] as cate_5425,kv['cate_5426'] as cate_5426,kv['cate_5427'] as cate_5427,kv['cate_5428'] as cate_5428,kv['cate_5429'] as cate_5429,kv['cate_5430'] as cate_5430,kv['cate_5431'] as cate_5431,kv['cate_5432'] as cate_5432,kv['cate_5433'] as cate_5433,kv['cate_5434'] as cate_5434,kv['cate_5435'] as cate_5435,kv['cate_5436'] as cate_5436,kv['cate_5437'] as cate_5437,kv['cate_5438'] as cate_5438,kv['cate_5439'] as cate_5439,kv['cate_5440'] as cate_5440,kv['cate_5441'] as cate_5441,kv['cate_5442'] as cate_5442,kv['cate_5443'] as cate_5443,kv['cate_5444'] as cate_5444,kv['cate_5445'] as cate_5445,kv['cate_5446'] as cate_5446,kv['cate_5462'] as cate_5462,kv['cate_5463'] as cate_5463,kv['cate_5464'] as cate_5464,kv['cate_5465'] as cate_5465,kv['cate_5466'] as cate_5466,kv['cate_5468'] as cate_5468,kv['cate_5469'] as cate_5469,kv['cate_5470'] as cate_5470,kv['cate_5473'] as cate_5473,kv['cate_5474'] as cate_5474,kv['cate_5475'] as cate_5475,kv['cate_5476'] as cate_5476,kv['cate_5477'] as cate_5477,kv['cate_5482'] as cate_5482,kv['cate_5510'] as cate_5510,kv['cate_5511'] as cate_5511,kv['cate_5527'] as cate_5527,kv['cate_5528'] as cate_5528,kv['cate_5532'] as cate_5532,kv['cate_5539'] as cate_5539
FROM (
  SELECT user_proxy_key, to_map(key, value) kv
  FROM recommend.FEATURE_CLICK_DRAFT1
  GROUP BY user_proxy_key
) t;
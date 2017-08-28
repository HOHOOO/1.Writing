packagecom.oserp.hiveudf;

importorg.apache.hadoop.hive.ql.exec.UDAF;

importorg.apache.hadoop.hive.ql.exec.UDAFEvaluator;

importorg.apache.hadoop.hive.serde2.io.DoubleWritable;

importorg.apache.hadoop.io.IntWritable;



publicclass HiveAvgextends UDAF {



        public staticclass AvgEvaluate implements UDAFEvaluator

        {

                public staticclass PartialResult

                {

                        public intcount;

                        public doubletotal;



                        public PartialResult()

                        {

                                count = 0;

                                total = 0;

                        }

                }



                private PartialResultpartialResult;



                @Override

                public voidinit() {

                        partialResult = new PartialResult();

                }



                public booleaniterate(IntWritable value)

                {

                        // 此处一定要判断partialResult是否为空，否则会报错

                        // 原因就是init函数只会被调用一遍，不会为每个部分聚集操作去做初始化

                        //此处如果不加判断就会出错

                        if (partialResult==null)

                        {

                                partialResult =new PartialResult();

                        }



                        if (value !=null)

                        {

                                partialResult.total =partialResult.total +value.get();

                                partialResult.count=partialResult.count + 1;

                        }



                        return true;

                }



                public PartialResult terminatePartial()

                {

                        returnpartialResult;

                }



                public booleanmerge(PartialResult other)

                {

                        partialResult.total=partialResult.total + other.total;

                        partialResult.count=partialResult.count + other.count;



                        return true;

                }



                public DoubleWritable terminate()

                {

                        return newDoubleWritable(partialResult.total /partialResult.count);

                }

        }

}

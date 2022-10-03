if [ $(hostname) == 'iccluster028' ]; 
then  
    ICCLUSTER=hdfs://iccluster028.iccluster.epfl.ch:8020
    export ML100Ku2base=$ICCLUSTER/cs449/data/ml-100k/u2.base;
    export ML100Ku2test=$ICCLUSTER/cs449/data/ml-100k/u2.test;
    export ML100Kudata=$ICCLUSTER/cs449/data/ml-100k/u.data;
    export ML1Mrbtrain=$ICCLUSTER/cs449/data/ml-1m/rb.train;
    export ML1Mrbtest=$ICCLUSTER/cs449/data/ml-1m/rb.test;
    export SPARKMASTER='yarn'
else 
    export ML100Ku2base=data/ml-100k/u2.base;
    export ML100Ku2test=data/ml-100k/u2.test;
    export ML100Kudata=data/ml-100k/u.data;
    export ML1Mrbtrain=data/ml-1m/rb.train;
    export ML1Mrbtest=data/ml-1m/rb.test;
    export SPARKMASTER='local[4]'
fi;

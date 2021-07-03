from pyspark import SparkContext, SparkConf
import sys
import json
class Task3:
    
    def main(self):
        def bid_partitioner(bid):
            key = bid[0] + bid[-1]
            return hash(key)
        review_file_path = sys.argv[1]
        output_file_path = sys.argv[2]
        partition_type = sys.argv[3]
        num_partitions = int(sys.argv[4])
        n = int(sys.argv[5])
        
        sc = SparkContext.getOrCreate()
        sc.setLogLevel("OFF")
        review_data_rdd = sc.textFile(review_file_path).map(lambda row: json.loads(row))
        bid_cnt_rdd = review_data_rdd.map(lambda json_record: (json_record["business_id"], 1))

        if partition_type != 'default':
            bid_cnt_rdd = bid_cnt_rdd.partitionBy(num_partitions, bid_partitioner)
        
        result = {}
        result['n_partitions'] = bid_cnt_rdd.getNumPartitions()
        result['n_items'] = bid_cnt_rdd.glom().map(len).collect()
        result['result'] = bid_cnt_rdd\
            .reduceByKey(lambda val1, val2: val1 + val2)\
            .filter(lambda tup: tup[1] > n)\
            .collect()

        with open(output_file_path, "w+") as of:
            json.dump(result, of)
        of.close()

if __name__ == "__main__":
    t3 = Task3()
    t3.main()
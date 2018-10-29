import shutil
import os
import hdfs
import random
import pyspark
import pickle as pkl


def spark_session() -> pyspark.sql.SparkSession:
    from pyspark.sql import SparkSession
    #config('spark.kryoserializer.buffer.max', '2g'). \
    spark = SparkSession.builder. \
        appName("pubmed spark"). \
        config('spark.executor.memory', '10g'). \
        config('spark.serializer', 'org.apache.spark.serializer.KryoSerializer'). \
        config('spark.kryoserializer.buffer.max', '1500m'). \
        config('spark.driver.maxResultSize', '2g'). \
        config('spark.driver.memory', '2g'). \
        config('spark.master', 'yarn'). \
        config('spark.executor.cores', '7'). \
        config('spark.executor.instances', '2'). \
        config('spark.driver.memoryOverhead', '2g'). \
        config('spark.executor.memoryOverhead', '2g'). \
        config('spark.submit.deployMode', 'client').getOrCreate()

    print("Spark session loaded!")

    return spark


def load_bioconcept(spark):

    return spark.read.load("/data/table/new_id_bioconcepts")


def load_medline(spark) -> pyspark.sql.DataFrame:
    medline = spark.read.load("/data/table/medline.parquet")
    return medline


def load_flat_text(spark):
    flat_text = spark.read.load("/data/table/flat_text")

    return flat_text


def load_100_pubmed():
    #flat_text = load_flat_text(spark)
    #pmid_df = flat_text.select("pmid").sample(100 / flat_text.count())
    #pmids = pmid_df.collect()
    #pmids = [xx.pmid for xx in pmids]
    #pubmed_df = flat_text.filter(flat_text.pmid.isin(pmids)) \
    #    .select("pmid", "word", "sent_pos", "word_pos").collect()
    #pkl.dump(file=open("./data/random_pubmed_words.pkl", 'wb'), obj=pubmed_df)

    pubmed_df = pkl.load(open("./data/random_pubmed_words.pkl", 'rb'))
    return pubmed_df


def get_some_pubmed_text():

    output_file = "./data/pubmed_word_format_100.txt"
    pubmed_df = load_100_pubmed()
    sent_end = False
    with open(output_file, 'w') as fw:
        for word in sorted(pubmed_df,
                           key=lambda x: [x.pmid, x.sent_pos, x.word_pos]):
            if word.word_pos == 0:
                fw.write("\n")
                fw.write("\n")
            fw.write(word.word + " ")


def spark_df_to_local_txt(local_path, df:pyspark.sql.DataFrame=None, deli="\t", hdfs_dir_path=None):
    def row_to_str(row):
        return deli.join([str(xx) for xx in row.asDict().values()])

    # map as string rdd
    if hdfs_dir_path == None:
        str_rdd = df.rdd.map(row_to_str)
        rand_bits = random.getrandbits(64)
        hdfs_dir_name = "str_df_%016x" % rand_bits
        hdfs_dir_path = "/tmp/"+hdfs_dir_name
        # save to hdfs
        str_rdd.saveAsTextFile(hdfs_dir_path)
    else:
        hdfs_dir_name = os.path.basename(hdfs_dir_path)
        hdfs_dir_path = "/tmp/"+hdfs_dir_name

    # get hdfs to local
    local_tmp_dir = "./tmp/"+hdfs_dir_name
    c = hdfs.Client("http://soldier1:50070")
    c.download(hdfs_dir_path, local_tmp_dir)
    # delete tmp hdfs
    c.delete(hdfs_dir_path, recursive=True)

    # cat to one file
    with open(local_path, 'wb') as outfile:
        for tmp_f in os.listdir(local_tmp_dir):
            fn = os.path.join(local_tmp_dir, tmp_f)
            with open(fn, 'rb') as readfile:
                shutil.copyfileobj(readfile, outfile)

    # remove local tmp_file
    shutil.rmtree(local_tmp_dir)


def pubmed_gene(spark):
    bioconcepts = load_bioconcept(spark)
    # get distinct pmids
    gene_related_pmid = bioconcepts.filter("concept_type == 'Gene'").select("pmid").distinct().cache()
    # get word-wise text
    flat_text = load_flat_text(spark)
    # limited text
    flat_text_gene = flat_text.join(gene_related_pmid, on="pmid")
    # df to local
    outfile = "./data/flat_text_gene.tsv"
    hdfs_path = "/tmp/str_df_adc8c15ad9b4a922"
    spark_df_to_local_txt(outfile, hdfs_dir_path=hdfs_path)


if __name__ == "__main__":
    spark = spark_session()
    pubmed_gene(spark)

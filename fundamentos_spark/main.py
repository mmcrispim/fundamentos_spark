# import libraries & init spark session
from pyspark.sql import SparkSession

def main():
    # Inicializa a sessão Spark
    #spark = SparkSession.builder.appName("Basic Test").getOrCreate()
    spark = SparkSession.builder \
        .appName("PySpark Installation Test") \
        .config("spark.eventLog.enabled", "false") \
        .getOrCreate()

    # Ajusta o nível de log para DEBUG
    #spark.sparkContext.setLogLevel("DEBUG")
    
    # load data
    print("Lendo o arquivo e criando o df_device")
    df_device = spark.read.json("/workspace/Files/device_part_1.json")
    print("Leu o arquivo e criou o df_device")
    df_device.show()
    
    # Mensagem de validação
    print("O processo foi executado com sucesso!")

if __name__ == "__main__":
    main()

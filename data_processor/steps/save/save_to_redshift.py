from data_processor.utils.step_reader import get_overwrite


class SaveToRedshift:

    def execute(self, params):
        params.data_frame.write.format("io.github.spark-redshift-community") \
            .option("url",
                    "jdbc:redshift://monjuviredshiftcluster.cmnf8atl4hzt.ap-south-1.redshift.amazonaws.com:5439/idpmorphosys?user=idpmorp&password=(ip%^1*927morP)") \
            .option("dbtable", params.model.table_name + "stage") \
            .option("tempdir", "D:/shared/temp/") \
            .mode("overwrite") \
            .save()

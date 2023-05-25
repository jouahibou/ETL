import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

val spark = SparkSession.builder().appName("MatchStats").getOrCreate()

// Définir le schéma du DataFrame des matchs
val matchSchema = StructType(Seq(
StructField("Match ID", IntegerType, nullable = false),
StructField("Team Name", StringType, nullable = false),
StructField("Team ID", IntegerType, nullable = false),
StructField("Points Scored", IntegerType, nullable = false)
))

// Charger les données des matchs depuis le fichier JSON
val gamesDF = spark.read.schema(matchSchema).json("game1.json")

// Afficher le DataFrame des matchs avec les points marqués par équipe, l'ID du match, le nom et l'ID de l'équipe
gamesDF.show()

// Définir le schéma du DataFrame des statistiques des joueurs
val statsSchema = StructType(Seq(
StructField("Team_full_name", StringType, nullable = false),
StructField("Player_first_name", StringType, nullable = false),
StructField("Player_last_name", StringType, nullable = false),
StructField("Pass", IntegerType, nullable = false),
StructField("Block_shoot", IntegerType, nullable = false),
StructField("Turnover", IntegerType, nullable = false)
))

// Charger les données des statistiques des joueurs depuis le fichier JSON
val statsDF = spark.read.schema(statsSchema).json("stats.json")

// Grouper les statistiques par équipe et ID du match
val groupedStatsDF = statsDF.groupBy("Team_full_name", "Match ID").sum("Pass", "Block_shoot", "Turnover")

// Afficher le DataFrame des statistiques groupées
groupedStatsDF.show()


val mergedDF = groupedStatsDF.join(gamesDF, Seq("Match ID"), "inner")

// Afficher le DataFrame fusionné
mergedDF.show()

val outputPath = "merged_data.csv" 

// Enregistrer le DataFrame fusionné au format CSV
mergedDF.write.csv(outputPath)




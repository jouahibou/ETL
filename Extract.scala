import requests
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Success, Failure}
import java.io.FileWriter

object Main extends App {
val gamesUrl = "https://www.balldontlie.io/api/v1/games"
val statsUrl = "https://www.balldontlie.io/api/v1/stats"
var gamePage = 1
var allGames = List.empty[Map[String, Any]]
var allStats = List.empty[Map[String, Any]]

val teams = List("Phoenix Suns", "Atlanta Hawks", "Los Angeles Clippers", "Milwaukee Bucks")

def fetchGames(page: Int): Future[Map[String, Any]] = {
val params = Map("page" -> page.toString)
val response = requests.get(gamesUrl, params=params)
response.json
}

def fetchStats(page: Int): Future[Map[String, Any]] = {
val params = Map("per_page" -> "100", "page" -> page.toString)
val response = requests.get(statsUrl, params=params)
response.json
}

def collectMatches(data: Map[String, Any]): List[Map[String, Any]] = {
val matches = data("data").asInstanceOf[List[Map[String, Any]]].filter { matchData =>
val season = matchData("season").asInstanceOf[Int]
val homeTeam = matchData("home_team")("full_name").asInstanceOf[String]
val visitorTeam = matchData("visitor_team")("full_name").asInstanceOf[String]

  season == 2021 && (teams.contains(homeTeam) || teams.contains(visitorTeam))
}

matches
}

def saveGamesToFile(games: List[Map[String, Any]], filename: String): Unit = {
val json = JSONArray(games).toString()
val writer = new FileWriter(filename)
writer.write(json)
writer.close()
}

def saveStatsToFile(stats: List[Map[String, Any]], filename: String): Unit = {
val json = JSONArray(stats).toString()
val writer = new FileWriter(filename)
writer.write(json)
writer.close()
}

def processGamesPage(page: Int): Unit = {
fetchGames(page).onComplete {
case Success(data) =>
val matches = collectMatches(data)
allGames = allGames ::: matches

    if (page < data("meta")("total_pages").asInstanceOf[Int]) {
      Thread.sleep(2000)
      processGamesPage(page + 1)
    } else {
      saveGamesToFile(allGames, "games.json")
      processStatsPage(1)
    }

  case Failure(exception) =>
    println(s"An error occurred: ${exception.getMessage}")
}
}

def processStatsPage(page: Int): Unit = {
fetchStats(page).onComplete {
case Success(data) =>
val stats = data("data").asInstanceOf[List[Map[String, Any]]]
allStats = allStats ::: stats


    if (page < data("meta")("total_pages").asInstanceOf[Int]) {
      Thread.sleep(2000)
      processStatsPage(page + 1)
    } else {
      val filteredStats = allStats.filter { stat =>
        val gameID = stat("game")("id").asInstanceOf[Int]
        allGames.exists(game => game("id") == gameID)
      }

      saveStatsToFile(filteredStats, "stats.json")
    }

  case Failure(exception) =>
    println(s"An error occurred: ${exception.getMessage}")
}
}

processGamesPage(gamePage)
}

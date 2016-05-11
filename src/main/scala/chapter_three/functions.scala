val rawUserArtistData = sc.textFile("file:///home/vagrant/sync/profiledata_06-May-2005/user_artist_data.txt")

val rawArtistData = sc.textFile("file:///home/vagrant/sync/profiledata_06-May-2005/artist_data.txt")

val rawArtistAlias = sc.textFile("file:///home/vagrant/sync/profiledata_06-May-2005/artist_alias.txt")

val artistById = rawArtistData.flatMap { line =>
  val (id, name) = line.span(_ != '\t')
  if (name.isEmpty) {
    None
  }  else {
    try {
        Some((id.toInt, name.trim))
    } catch {
      case e: NumberFormatException => None
    }
  }
}

val artistAlias = rawArtistAlias.flatMap { line =>
  val tokens = line.split('\t')
  if (tokens(0).isEmpty) {
    None
  } else {
    Some((tokens(0).toInt, tokens(1).toInt))
  }
}.collectAsMap()
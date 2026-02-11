// Increase timeout for Sonatype uploads
Global / concurrentRestrictions := Seq(
  Tags.limitAll(1)
)

// Increase HTTP timeout
import scala.concurrent.duration._
ThisBuild / updateOptions := updateOptions.value.withGigahorse(false)

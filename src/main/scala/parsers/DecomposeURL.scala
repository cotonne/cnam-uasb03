package parsers

import models.ApacheLog

object DecomposeURL {
  def apply(apacheLog: ApacheLog): ApacheLog = {
    // requestFirstLine = "POST /administrator/index.php HTTP/1.1"
    val s = apacheLog.requestFirstLine.split(" ")
    apacheLog.copy(
      method = s(0),
      page = s(1),
      protocol = s(2)
    )
  }
}

package parsers

import models.ApacheLog
import nl.basjes.parse.useragent.UserAgentAnalyzer

object EnrichUserAgent {
  private val uaa = UserAgentAnalyzer
    .newBuilder()
    .withCache(1234)
    .withFields(
      "OperatingSystemClass",
      "OperatingSystemNameVersion",
      "AgentClass",
      "AgentNameVersionMajor")
    .withAllFields()
    .build

  def apply(apacheLog: ApacheLog): ApacheLog = {
    val userAgent = uaa.parse(apacheLog.requestHeaderUserAgent)
    apacheLog.copy(
      operatingSystemClass = userAgent.get("OperatingSystemClass").getValue,
      operatingSystemNameVersion = userAgent.get("OperatingSystemNameVersion").getValue,
      agentClass = userAgent.get("AgentClass").getValue,
      agentNameVersionMajor = userAgent.get("AgentNameVersionMajor").getValue
    )
  }
}

package models

import java.time.LocalDateTime

case class ApacheLog(remoteHost: String,
                     isoCode: String,
                     remoteUser: String,
                     timeReceived: LocalDateTime,
                     requestFirstLine: String,
                     method: String,
                     page: String,
                     protocol: String,
                     status: Int,
                     responseBytes: Int,
                     requestHeaderReferer: String,
                     requestHeaderUserAgent: String,
                     operatingSystemClass: String,
                     operatingSystemNameVersion: String,
                     agentClass: String,
                     agentNameVersionMajor: String,
                     remoteLogName: String)

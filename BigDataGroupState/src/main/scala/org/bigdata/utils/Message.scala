package org.bigdata.utils

case class Message(
                      messageid:       String             = Util.uuid,
                      timestamp:       java.sql.Timestamp = Util.localToGMT(),
                      plot:           String              = "",
                      state:          String             = "")


case class SpotState(
                      spotId: String,
                      var activity: String,
                      var start: java.sql.Timestamp,
                      var end: java.sql.Timestamp,
                      var parking: Boolean = false,
                      var time: Long = 0
                    )

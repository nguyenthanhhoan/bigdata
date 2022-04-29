package org.bigdata.stream

import org.bigdata.utils.{Message, SpotState}

class StateMgmt() extends Serializable {

//    @transient lazy val log = org.apache.log4j.LogManager.getLogger(getClass.getCanonicalName)

    val interval = 60 * 1000 * 30 //30 minutes

//    log.info("spot interval set to " + interval)

    import org.apache.spark.sql.streaming.GroupState


    def spotTimeCount(
                       spotId: String,
                       inputs:    Iterator[Message],
                       oldState:  GroupState[SpotState]): SpotState = {

      if (oldState.hasTimedOut) {

        //log.info("state time out : " + oldState.get.activity)

        val s = oldState.get
        s.parking = false
        oldState.remove

        s

      } else {

        var state: SpotState = if (oldState.exists) oldState.get else SpotState(
          spotId,
          "",
          null,
          null)
        // we simply specify an old date that we can compare against and
        // immediately update based on the values in our data

        for (input <- inputs) {
          //        log.error(input.event.`type` + " ---- " + state.activity)
          if (input.state == "parked") {
            if (state.start == null) {
              state.start = input.timestamp
            }
            state.end = input.timestamp
            state.time = (input.timestamp.getTime - state.start.getTime)/60000
            state.activity = "parked"
            state.parking = true
            oldState.update(state)
            oldState.setTimeoutDuration(interval)
          } else if (input.state == "moving" && state.activity == "parked") {

            //          log.error("time diff : " + (input.timestamp.getTime - state.start.getTime))
            if (input.timestamp.after(state.start)) {
              state.end = input.timestamp
              state.time = (input.timestamp.getTime - state.start.getTime)/60000
            }
            state.activity = "exit"
            oldState.update(state)
            oldState.setTimeoutDuration(60000) //1 minute
          }
          //exit only nothing to do
        }
        state

      }
    }
}

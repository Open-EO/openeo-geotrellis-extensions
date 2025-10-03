package org.openeo.geotrellis

import jep.{JepConfig, SharedInterpreter}

package object udf {
  object SharedInterpreterFactory {
    private var isInterpreterInitialized = false

    def create(): SharedInterpreter = {
      if (!isInterpreterInitialized) {
        val config = new JepConfig()
        config.redirectStdErr(System.err)
        config.redirectStdout(System.out)
        SharedInterpreter.setConfig(config)
        isInterpreterInitialized = true
      }
      new SharedInterpreter
    }

  }
}

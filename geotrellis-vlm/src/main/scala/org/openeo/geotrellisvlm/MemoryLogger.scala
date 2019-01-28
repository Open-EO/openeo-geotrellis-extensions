package org.openeo.geotrellisvlm

import java.io.{FileOutputStream, PrintStream}
import java.lang.management.ManagementFactory

import org.apache.hadoop.yarn.util.ProcfsBasedProcessTree

class MemoryLogger(name: String) {

  val pidReg = raw"(\d*).*".r
  val pidReg(pid) = ManagementFactory.getRuntimeMXBean.getName
  val processTree = new ProcfsBasedProcessTree(pid)

  def logMem() {
    processTree.updateProcessTree()

    val stream = new PrintStream(new FileOutputStream("/home/niels/memory.txt", true))
    try {
      stream.append(s"$name-cpuTime: ${processTree.getCumulativeCpuTime()}${System.lineSeparator()}")
      stream.append(s"$name-rssMem: ${processTree.getCumulativeRssmem()}${System.lineSeparator()}")
      stream.append(s"$name-vMem: ${processTree.getCumulativeVmem()}${System.lineSeparator()}")
    } finally {
      stream.close()
    }
  }
}

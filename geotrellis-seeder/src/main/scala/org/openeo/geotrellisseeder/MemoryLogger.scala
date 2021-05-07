package org.openeo.geotrellisseeder

import java.io.{FileOutputStream, PrintStream}
import java.lang.management.ManagementFactory

import org.apache.hadoop.yarn.util.ProcfsBasedProcessTree

class MemoryLogger(name: String) {

  val pidReg = raw"(\d*).*".r
  val pidReg(pid) = ManagementFactory.getRuntimeMXBean.getName
  val processTree = new ProcfsBasedProcessTree(pid)

  def logMem() {
    processTree.updateProcessTree()

    val stream = new PrintStream(new FileOutputStream(System.getProperty("user.home") + "/memory.txt", false))
    try {
      stream.println(s"$name-cpuTime: ${processTree.getCumulativeCpuTime()}")
      stream.println(s"$name-rssMem: ${processTree.getRssMemorySize()}")
      stream.println(s"$name-vMem: ${processTree.getVirtualMemorySize()}")
    } finally {
      stream.close()
    }
  }
}

class FourInts(first: Int, second: Int, third: Int, fourth: Int) extends Serializable {

  def getFirst(): Int = this.first
  def getSecond(): Int = this.second
  def getThird(): Int = this.third
  def getFourth(): Int = this.fourth
  def getValue(): Int = {
    this.getSecond() + this.getThird() + this.getFourth()
  }

  override
  def toString: String = {
    this.getFirst().toString + " | " + this.getSecond().toString + " | " + this.getThird + " | " + this.getFourth
  }
}

object FourInts {
  def cmpKey(o: FourInts): Int = {
    o.getFirst()
  }
}

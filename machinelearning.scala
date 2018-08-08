
val v = scala.collection.immutable.Vector.empty
println(v)

// Add elements from List of Ints to end of vector.
val v2 = v ++ List(10, 20, 30)
println(v2)

import scala.collection.immutable.Vector

class LinearRegression {
 
   private var computedGradient: Vector[Double] = Vector.fill(2){0}
 
   def fit(x: Vector[Double], y: Vector[Double], lr: Float, iters: Int): Vector[Double] = {
      val thetas: Vector[Double] = Vector.fill(y.length){0}
      computedGradient = computeGradient(x, y, thetas, lr, iters)
      computedGradient
   }
 
   def predict(x: Double): Double = {
      val this_xj = v :+ 1.0 :+ x
      computedGradient.zip(this_xj).map { case (x, y) => x * y }.sum // vector multiplication
   }
 
   private def computeGradient(x: Vector[Double], y: Vector[Double], thetas: Vector[Double], lr: Float, iterations: Int): Vector[Double] = {
      val nbOfTrainingExamples = x.length
      val computedThetas = (0 to iterations).foldLeft(thetas)({
         case (thetas, i) =>
            var updatedThetas = thetas
            (0 to x.length-1).map  { j =>
                val this_xj = v :+ 1.0 :+ x(j)
                val predicted = updatedThetas.zip(this_xj).map { case (x, y) => x * y }.sum // vector multiplication
                val error = predicted - y(j)
                val grad = error * x(j)/nbOfTrainingExamples
                updatedThetas = updatedThetas.map { _ - grad*lr }
            }
            updatedThetas
      })
      computedThetas
   }
 
}

val vec = scala.collection.immutable.Vector.empty
val vec_x = vec :+ 1.0 :+ 2.0
val vec_y = vec :+ 2.0 :+ 4.0
val thetas = vec :+ 1.0 :+ 1.0

val lr = 0.02f
val iterations = 10000

val model = new LinearRegression()
val params = model.fit(vec_x, vec_y, lr, iterations)
val predictions = model.predict(3)

System.exit(0)

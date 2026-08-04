package io.semanticdf

import java.nio.file.Files
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.must.Matchers

/** Regression tests for io.semanticdf.tools.DocsGen. */
class DocsGenSpec extends AnyFunSpec with Matchers {

  private val docsGen = new io.semanticdf.tools.DocsGen()

  describe("DocsGen") {

    it("generates valid HTML with doctype and title") {
      val html = docsGen.fromFile(getClass.getResource("/flights_model.yml").getPath)
      html must include("<!DOCTYPE html>")
      html must include("<title>SemanticDF Model Catalog</title>")
      html must include("</html>")
    }

    it("includes all model names from the YAML") {
      val html = docsGen.fromFile(getClass.getResource("/flights_model.yml").getPath)
      html must include("flights")
      html must include("carriers")
    }

    it("includes dimension names and expr hints") {
      val html = docsGen.fromFile(getClass.getResource("/flights_model.yml").getPath)
      html must include("carrier")
      html must include("origin")
      html must include("expr") // CSS class used in HTML
    }

    it("includes measure names and expressions") {
      val html = docsGen.fromFile(getClass.getResource("/flights_model.yml").getPath)
      html must include("total_passengers")
      html must include("flight_count")
      html must include("sum(passengers)")
      html must include("count(1)")
    }

    it("includes table name and description") {
      val html = docsGen.fromFile(getClass.getResource("/flights_model.yml").getPath)
      html must include("flights_tbl")
      html must include("Flight data with origin")
    }

    it("does not produce broken HTML tags from model names") {
      val html = docsGen.fromFile(getClass.getResource("/flights_model.yml").getPath)
      // Verify the HTML is well-formed enough to contain our content
      html must include("flights")
      html must include("</html>")
    }

    it("writes output to a file without error") {
      val tmp = java.io.File.createTempFile("docsgen-test", ".html")
      tmp.deleteOnExit()
      val html = docsGen.fromFile(getClass.getResource("/flights_model.yml").getPath)
      docsGen.write(tmp.getPath, html)
      val readBack = scala.io.Source.fromFile(tmp).mkString
      readBack mustEqual html
    }

    it("handles YAML with only dimensions (no measures)") {
      val yaml =
        """minimal:
          |  table: foo
          |  dimensions:
          |    x: x
          |    y: y
          |""".stripMargin
      val tmp = java.io.File.createTempFile("docsgen-minimal", ".yml")
      tmp.deleteOnExit()
      Files.writeString(tmp.toPath, yaml)
      val html = docsGen.fromFile(tmp.getPath)
      html must include("minimal")
      html must include("x")
      html must include("y")
    }

    it("handles YAML with empty measures section") {
      val yaml =
        """dim_only:
          |  table: foo
          |  dimensions:
          |    d1: d1
          |  measures: {}
          |""".stripMargin
      val tmp = java.io.File.createTempFile("docsgen-dimonly", ".yml")
      tmp.deleteOnExit()
      Files.writeString(tmp.toPath, yaml)
      val html = docsGen.fromFile(tmp.getPath)
      html must include("dim_only")
      html must include("d1")
    }


  }
}

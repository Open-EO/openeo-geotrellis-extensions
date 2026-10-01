package org.openeo.geotrellis.onnx

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}


/** * Descriptor for a single input or output tensor, as declared in an MLM STAC item. * * @param name      Tensor name used as ONNX session feed key. * @param dataType  Data type string, e.g. "float32", "int32". * @param shape     Tensor shape; -1 denotes a dynamic dimension. * @param axes      Optional axis semantic labels, e.g. Seq("batch","bands","height","width"). */
case class TensorSpec(
                       name: String,
                       dataType: String,
                       shape: Seq[Long],
                       dimOrder: Seq[String] = Seq.empty
                     )

/** * A class descriptor for output classification. * * @param value Numeric class index. * @param name  Human-readable class label. */
case class MlmClass(value: Int, name: String)

/** * All runtime-relevant metadata extracted from an MLM STAC item. * * @param modelAssetHref  Relative or absolute URI to the ONNX model asset. * @param modelName       Human-readable model name from mlm:name. * @param framework       Framework name, e.g. "onnx". * @param tasks           List of MLM tasks, e.g. Seq("semantic-segmentation"). * @param inputs          Declared input tensor specs. * @param outputs         Declared output tensor specs. * @param classes         Optional output class definitions. */
case class MlmModelDescriptor(
                               modelAssetHref: String,
                               modelName: String,
                               framework: Option[String],
                               tasks: Seq[String],
                               inputs: Seq[TensorSpec],
                               outputs: Seq[TensorSpec],
                               classes: Seq[MlmClass] = Seq.empty
                             )

/** * Parses an MLM STAC item JSON into an [[MlmModelDescriptor]]. * * Spec reference: https://github.com/stac-extensions/mlm */
object StacModelParser {
  private val mapper = new ObjectMapper()

  /**   * Parse a raw JSON string representing a STAC item with the MLM extension.   *   * @param stacJson  Full STAC item JSON as a string.   * @return          Parsed [[MlmModelDescriptor]].   * @throws IllegalArgumentException if required fields are missing or malformed.   */
  def parse(stacJson: String): MlmModelDescriptor = {
    val root = mapper.readTree(stacJson)
    parseNode(root)
  }

  /**   * Parse from a [[JsonNode]] directly (useful when already parsed elsewhere).   */
  def parseNode(root: JsonNode): MlmModelDescriptor = {
    val props = requireField(root, "properties", "STAC item must have a 'properties' object")

    val modelName = requireTextField(props, "mlm:name", "properties.mlm:name is required")

    val framework = Option(props.get("mlm:framework"))
      .filter(_.isArray)
      .flatMap(arr => Option(arr.get(0)))
      .flatMap(f => Option(f.get("name")))
      .map(_.asText())

    val tasks = Option(props.get("mlm:tasks"))
      .filter(_.isArray)
      .map(arr => (0 until arr.size()).map(i => arr.get(i).asText()).toSeq)
      .getOrElse(Seq.empty)

    val inputs = parseTensorSpecs(props, "mlm:input", required = true)
    val outputs = parseTensorSpecs(props, "mlm:output", required = true)

    val classes = Option(props.get("mlm:classes"))
      .filter(_.isArray)
      .map { arr =>
        (0 until arr.size()).map { i =>
          val cls = arr.get(i)
          MlmClass(
            value = requireIntField(cls, "value", s"mlm:classes[$i].value is required"),
            name  = requireTextField(cls, "name", s"mlm:classes[$i].name is required")
          )
        }.toSeq
      }
      .getOrElse(Seq.empty)

    val modelAssetHref = parseModelAssetHref(root)

    MlmModelDescriptor(
      modelAssetHref = modelAssetHref,
      modelName      = modelName,
      framework      = framework,
      tasks          = tasks,
      inputs         = inputs,
      outputs        = outputs,
      classes        = classes
    )
  }


  private def parseTensorSpecs(props: JsonNode, field: String, required: Boolean): Seq[TensorSpec] = {
    val node = props.get(field)
    if (node == null || node.isNull) {
      if (required)
        throw new IllegalArgumentException(s"properties.$field is required but missing")
      else
        return Seq.empty
    }
    if (!node.isArray)
      throw new IllegalArgumentException(s"properties.$field must be a JSON array")

    (0 until node.size()).map { i =>
      val t = node.get(i)
      val name     = requireTextField(t, "name",      s"$field[$i].name is required")
      val dataType = requireTextField(t, "data_type", s"$field[$i].data_type is required")

      val shape: Seq[Long] = Option(t.get("shape"))
        .filter(_.isArray)
        .map { arr =>
          (0 until arr.size()).map { j =>
            val elem = arr.get(j)
            if (elem.isNull || (elem.isTextual && elem.asText() == "N")) -1L
            else elem.asLong(-1L)
          }.toSeq
        }
        .getOrElse(Seq.empty)

      val dimOrder: Seq[String] = Option(t.get("dim_order"))
        .filter(_.isArray)
        .map(arr => (0 until arr.size()).map(j => arr.get(j).asText()).toSeq)
        .getOrElse(Seq.empty)

      TensorSpec(name = name, dataType = dataType, shape = shape, dimOrder = dimOrder)
    }.toSeq
  }

  private def parseModelAssetHref(root: JsonNode): String = {
    val assets = requireField(root, "assets", "STAC item must have an 'assets' object")
    // find the asset whose roles include "model" or "ml-model:checkpoint"
    val modelRoles = Set("mlm:model")
    val fields = assets.fields()
    while (fields.hasNext) {
      val entry = fields.next()
      val asset = entry.getValue
      val roles = Option(asset.get("roles"))
        .filter(_.isArray)
        .map(arr => (0 until arr.size()).map(i => arr.get(i).asText()).toSet)
        .getOrElse(Set.empty[String])
      if (roles.exists(modelRoles.contains)) {
        val href = requireTextField(asset, "href", s"asset '${entry.getKey}' must have an 'href'")
        if (href.startsWith("http")) return href
        else {
          val resource = getClass.getResource(href) // Only for testing purposes, in production this should be a proper URI resolution
          return resource.toString
        }
      }
    }
    throw new IllegalArgumentException(
      "No model asset found in 'assets'. Expected an asset with role 'mlm:model'."
    )
  }

  private def requireField(node: JsonNode, field: String, msg: String): JsonNode = {
    val child = node.get(field)
    if (child == null || child.isNull)
      throw new IllegalArgumentException(msg)
    child
  }

  private def requireTextField(node: JsonNode, field: String, msg: String): String = {
    val child = node.get(field)
    if (child == null || child.isNull || !child.isTextual)
      throw new IllegalArgumentException(msg)
    child.asText()
  }

  private def requireIntField(node: JsonNode, field: String, msg: String): Int = {
    val child = node.get(field)
    if (child == null || child.isNull || !child.isNumber)
      throw new IllegalArgumentException(msg)
    child.asInt()
  }

}

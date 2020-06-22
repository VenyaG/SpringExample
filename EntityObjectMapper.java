package com.example.core.objects.api;

import static com.example.core.common.Status.CREATE;
import static com.example.core.common.Status.DELETE;
import static com.example.core.model.entities.StandardField.ID;
import static com.example.core.model.entities.StandardField.NAME;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toMap;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.example.common.Utils;
import com.example.common.context.RequestContext;
import com.example.common.exceptions.UnprocessableException;
import com.example.common.time.DateTimeUtils;
import com.example.core.common.Status;
import com.example.core.layers.api.dto.AttributeResponse;
import com.example.core.layers.api.dto.FeatureField;
import com.example.core.layers.entities.Layer;
import com.example.core.layers.entities.LayerField;
import com.example.core.layers.entities.LayerType;
import com.example.core.layers.entities.objects.LayerObject;
import com.example.core.layers.entities.objects.LayerObjectAttribute;
import com.example.core.layers.exceptions.NotEntityTypeLayerException;
import com.example.core.model.EntityUtils;
import com.example.core.model.entities.EntityType;
import com.example.core.model.entities.Field;
import com.example.core.model.entities.FieldType;
import com.example.core.model.entities.fields.BaseField;
import com.example.core.model.entities.fields.GeometryField;
import com.example.core.model.entities.fields.RelationField;
import com.example.core.objects.AttributeFactory;
import com.example.core.objects.api.dto.EntityObjectDTO;
import com.example.core.objects.api.dto.EntityReference;
import com.example.core.objects.api.dto.Feature;
import com.example.core.objects.api.dto.ObjectAttachment;
import com.example.core.objects.entities.EntityObject;
import com.example.core.objects.entities.EntityObjectStatus;
import com.example.core.objects.entities.attributes.Attribute;
import com.example.core.objects.entities.attributes.BooleanAttribute;
import com.example.core.objects.entities.attributes.DateAttribute;
import com.example.core.objects.entities.attributes.DateTimeAttribute;
import com.example.core.objects.entities.attributes.Geometry;
import com.example.core.objects.entities.attributes.GeometryAttribute;
import com.example.core.objects.entities.attributes.NumericAttribute;
import com.example.core.objects.entities.attributes.RelationAttribute;
import com.example.core.objects.entities.attributes.StringAttribute;
import com.example.core.objects.entities.attributes.TimeAttribute;
import java.io.IOException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.NonNull;
import org.apache.commons.lang3.StringUtils;
import org.modelmapper.Converter;
import org.modelmapper.ModelMapper;

/**
 * Маппер между {@link EntityObject} и {@link EntityObjectDTO}
 */
public final class EntityObjectMapper {

  private static Function<UUID, String> GUID_OR_NULL = guid -> guid == null ? null
      : guid.toString();

  private static Converter<EntityObjectStatus, Integer> STATUS_INTEGER_CONVERTER =
      ctx -> ctx.getSource().ordinal();

  private static Converter<UUID, String> UUID_CONVERTER =
      ctx -> GUID_OR_NULL.apply(ctx.getSource());

  private static final EntityObjectMapper INSTANCE = new EntityObjectMapper();

  private ModelMapper mapper;

  private EntityObjectMapper() {
    mapper = new ModelMapper();
    mapper.createTypeMap(EntityObject.class, EntityObjectDTO.class)
        .addMappings(m -> m.skip((dto, o) -> dto.setAttributes(null)))
        .addMappings(m -> m.using(STATUS_INTEGER_CONVERTER)
            .map(EntityObject::getStatus, EntityObjectDTO::setStatus))
        .addMappings(m -> m.using(UUID_CONVERTER).
            map(EntityObject::getGuid, EntityObjectDTO::setGuid));
    mapper.createTypeMap(EntityObject.class, Feature.class)
        .addMappings(m -> m.using(STATUS_INTEGER_CONVERTER)
            .map(EntityObject::getStatus, Feature::setStatus));
    mapper.createTypeMap(LayerField.class, FeatureField.class);
  }

  /**
   * Конвертировать {@link EntityObject} в {@link EntityObjectDTO}
   *
   * @param entityType класс объектов
   * @param object объект
   * @return конвертированный dto {@link EntityObjectDTO}
   */
  public static EntityObjectDTO map(@NonNull EntityType entityType, @NonNull EntityObject object) {
    EntityObjectDTO dto = INSTANCE.mapper.map(object, EntityObjectDTO.class);
    Map<String, Object> valueMap = new LinkedHashMap<>();
    EntityUtils.attributeValueMap(entityType, object)
        .forEach((f, v) -> valueMap.put(f.getCodeName(), mapAttributeValue(f, v)));
    dto.setAttributes(valueMap);
    return dto;
  }

  /**
   * Конвертировать {@link EntityObject} в {@link Feature}
   */
  public static Feature mapFeature(@NonNull Layer layer, @NonNull EntityObject object) {
    if (layer.getLayerType() != LayerType.ENTITY_TYPE) {
      throw new NotEntityTypeLayerException();
    }
    Feature feature = INSTANCE.mapper.map(object, Feature.class);
    Map<String, Object> valueMap = new LinkedHashMap<>();
    GeometryField geometryField = layer.getGeometryField();
    EntityUtils.fullAttributeValueMap(geometryField.getEntityType(), object)
        .forEach((f, v) -> valueMap.put(f.getCodeName(), mapAttributeValue(f, v)));
    feature.setId(layer.getCodeName(), object.getId());
    feature.setProperties(valueMap);
    feature.setGeometry((String) valueMap.remove(geometryField.getCodeName()));
    return feature;
  }

  /**
   * Конвертировать {@link EntityObject} в {@link Feature}
   */
  public static List<FeatureField> mapFeatureFields(Layer layer) {
    List<FeatureField> fields = layer.getFields().stream()
        .map(f -> INSTANCE.mapper.map(f, FeatureField.class))
        .collect(Collectors.toList());
    if (layer.getLayerType() == LayerType.ENTITY_TYPE) {
      Map<String, BaseField> fieldMap = layer.getGeometryField().getEntityType().fieldMap();
      fields.forEach(f -> {
        boolean required = Optional.ofNullable(fieldMap.get(f.getCodeName()))
            .map(BaseField::isRequired).orElse(false);
        f.setRequired(required);
      });
    }
    return fields;
  }

  /**
   * Конвентировать json в {@link EntityObject}
   *
   * @param json json
   * @return конвертировнный {@link EntityObject}
   */
  public static EntityObject mapFromJson(@NonNull EntityType entityType, String json) {
    Utils.requireNonBlank(json, "object json is blank");
    try {
      return mapFromJson(entityType, new ObjectMapper().readTree(json));
    } catch (IOException e) {
      throw new UnprocessableException("Failed to parse object", e);
    }
  }

  /**
   * Конвентировать {@link JsonNode} в {@link EntityObject}
   *
   * @param json json
   * @return конвертировнный {@link EntityObject}
   */
  public static EntityObject mapFromJson(@NonNull EntityType entityType, @NonNull JsonNode json) {
    EntityObject object = readBaseObject(json);
    JsonNode attributes = json.get("attributes");
    entityType.getFields().forEach(field -> readAttribute(object, attributes, field));

    JsonNode attachments = json.get("attachments");
    object.setAttachments(readAttachmentObjects(attachments));

    return object;
  }

  private static List<ObjectAttachment> readAttachmentObjects(JsonNode attachments) {

    if (attachments == null) {
      return null;
    }

    List<ObjectAttachment> list = new ArrayList<>();
    try {
      for (final JsonNode objNode : attachments) {

        Status status = Status.valueOf(objNode.get("status").asText());
        if (status == CREATE || status == DELETE) {
          ObjectAttachment object = new ObjectAttachment();
          object.setCreateDate(Timestamp.valueOf(DateTimeUtils.now()).toString());
          object.setGuid(objNode.get("guid").asText());
          object.setStatus(status);
          list.add(object);
        }
      }
    } catch (Exception e) {
      throw new UnprocessableException("Failed to parse json object attachments", e);
    }
    return list;
  }

  /**
   * Конвертировать {@link LayerObject} в {@link EntityObject}
   *
   * @return конвертированный {@link EntityObject}
   */
  public static EntityObject convertLayerObject(@NonNull EntityType entityType,
                                                @NonNull String geomField,
                                                @NonNull LayerObject layerObject) {
    List<LayerObjectAttribute> layerAttributes = layerObject.getAttributes();

    EntityObject object = new EntityObject();
    object.setId(layerObject.getId());
    object.setName(layerObject.getName());
    object.setEntityType(entityType.getCodeName());
    object.setStatus(EntityObjectStatus.values()[layerObject.getStatus()]);
    object.getMetadata().changed(RequestContext.getUser());

    JsonNode attachments = new ObjectMapper().valueToTree(layerObject.getAttachments());
    object.setAttachments(readAttachmentObjects(attachments));

    Map<String, BaseField> fieldMap = entityType.fieldMap();

    Map<String, List<Attribute>> attributes = layerAttributes.stream()
        .filter(dto -> fieldMap.containsKey(dto.getCodeName()))
        .map(dto -> convertToEntityObjectAttribute(dto, fieldMap.get(dto.getCodeName())))
        .collect(toMap(AttributeResponse::getCodeName, AttributeResponse::getAttributes));
    attributes.put(geomField, singletonList(new GeometryAttribute(layerObject.getGeometry())));
    object.setAttributes(attributes);
    return object;
  }

  /**
   * Конвертировать {@link EntityObject} в {@link EntityReference}
   *
   * @param object объект
   * @return конвертированный {@link EntityReference}
   */
  public static EntityReference mapReference(@NonNull EntityObject object) {
    EntityReference reference = new EntityReference();
    reference.setId(object.getId());
    reference.setName(object.getName());
    reference.setStatus(object.getStatus().ordinal());
    reference.setEntityType(object.getEntityType());
    reference.setGuid(GUID_OR_NULL.apply(object.getGuid()));
    return reference;
  }

  /**
   * Получить список файлов объекта из json
   *
   * @param value json-строка
   * @return список файлов объекта
   * @throws UnprocessableException если невалидный json
   */
  public static List<ObjectAttachment> mapJsonToAttachments(String value) {
    try {
      return new ArrayList<>(
          Arrays.asList(new ObjectMapper().readValue(value, ObjectAttachment[].class)));
    } catch (IOException e) {
      throw new UnprocessableException("Failed to parse json attachments string", e);
    }
  }

  /**
   * Конвертировать объект в строку
   *
   * @param value json-строка
   * @return строка
   * @throws UnprocessableException ошибка при конвертации
   */
  public static String mapToJsonString(Object value) {
    try {
      return new ObjectMapper().writeValueAsString(value);
    } catch (JsonProcessingException e) {
      throw new UnprocessableException("Failed to convert object to json string", e);
    }
  }

  private static Object mapAttributeValue(Field field, Object value) {
    if (value == null) {
      return null;
    }
    if (field.getFieldType() == FieldType.RELATION) {
      if (field.isMultiple()) {
        return Arrays.stream((Object[]) value)
            .map(o -> mapReference((EntityObject) o))
            .toArray();
      } else {
        return mapReference((EntityObject) value);
      }
    } else if (field.getFieldType() == FieldType.GEOMETRY) {
      return ((Geometry) value).getGeometry();
    }
    return value;
  }

  private static EntityObject readBaseObject(JsonNode parent) {
    EntityObject object = new EntityObject();
    object.setId(readNode(parent, ID.getCodeName()).map(JsonNode::asInt).orElse(0));
    object.setName(readNode(parent, NAME.getCodeName()).map(JsonNode::asText).orElse(null));
    object.setEntityType(readNode(parent, "entityType").map(JsonNode::asText).orElse(null));
    int status = readNode(parent, "status").map(JsonNode::asInt).orElse(0);
    object.setStatus(EntityObjectStatus.values()[status]);
    return object;
  }

  private static void readAttribute(EntityObject object, JsonNode parent, Field field) {
    JsonNode node = parent.get(field.getCodeName());
    if (node != null) {
      if (field.isMultiple()) {
        object.add(field.getCodeName(), readMultipleAttribute(node, field));
      } else {
        object.add(field.getCodeName(), readSingleAttribute(node, field));
      }
    }
  }

  private static List<Attribute> readMultipleAttribute(JsonNode node, Field field) {
    if (node.isNull()) {
      return Collections.emptyList();
    }
    List<Attribute> result = new ArrayList<>();
    node.iterator().forEachRemaining(child -> result.add(readSingleAttribute(child, field)));
    return result;
  }

  private static Attribute readSingleAttribute(JsonNode node, Field field) {
    return AttributeFactory.create(field.getFieldType(), readSingleValue(node, field));
  }

  private static <T> T readValueOrNull(JsonNode node, Supplier<T> valueSupplier) {
    return node.isNull() ? null : valueSupplier.get();
  }

  private static Object readSingleValue(JsonNode node, Field field) {
    FieldType fieldType = field.getFieldType();
    if (fieldType == FieldType.NUMERIC) {
      return readValueOrNull(node, node::asDouble);
    } else if (fieldType == FieldType.DATE) {
      return readValueOrNull(node, () -> LocalDate.parse(node.asText()));
    } else if (fieldType == FieldType.TIME) {
      return readValueOrNull(node, () -> LocalTime.parse(node.asText()));
    } else if (fieldType == FieldType.DATE_TIME) {
      return readValueOrNull(node, () -> LocalDateTime.parse(node.asText()));
    } else if (fieldType == FieldType.RELATION) {
      return readValueOrNull(node, () -> readBaseObject(node));
    } else if (fieldType == FieldType.BOOLEAN) {
      return readValueOrNull(node, node::asBoolean);
    } else {
      return readValueOrNull(node, node::asText);
    }
  }

  private static Optional<JsonNode> readNode(JsonNode parent, String field) {
    return Optional.ofNullable(parent.get(field));
  }

  public static AttributeResponse convertToEntityObjectAttribute(LayerObjectAttribute attribute,
                                                                  BaseField baseField) {
    FieldType fieldType = baseField.getFieldType();
    String fieldCode = baseField.getCodeName();

    String value = attribute.getValue();
    if (fieldType == FieldType.BOOLEAN) {
      return toAttributeResponse(fieldCode, new BooleanAttribute(
          readValueOrNull(value, () -> Boolean.parseBoolean(value))));
    } else if (fieldType == FieldType.NUMERIC) {
      return toAttributeResponse(fieldCode, new NumericAttribute(
          readValueOrNull(value, () -> Double.parseDouble(value))));
    } else if (fieldType == FieldType.DATE) {
      return toAttributeResponse(fieldCode, new DateAttribute(
          readValueOrNull(value, () -> toLocalDate(value))));
    } else if (fieldType == FieldType.DATE_TIME) {
      return toAttributeResponse(fieldCode, new DateTimeAttribute(
          readValueOrNull(value, () -> toLocalDateTime(value))));
    } else if (fieldType == FieldType.TIME) {
      return toAttributeResponse(fieldCode, new TimeAttribute(
          readValueOrNull(value, () -> toLocalTime(value))));
    } else {
      return toAttributeResponse(fieldCode, new StringAttribute(
          readValueOrNull(value, () -> value)));
    }
  }

  private static <T> T readValueOrNull(String value, Supplier<T> valueSupplier) {
    return StringUtils.isBlank(value) ? null : valueSupplier.get();
  }

  private static LocalDate toLocalDate(String value) {
    return toLocalDateTime(value).toLocalDate();
  }

  private static LocalDateTime toLocalDateTime(String value) {
    long milliseconds = Long.parseLong(value);
    return LocalDateTime.ofInstant(Instant.ofEpochMilli(milliseconds), ZoneId.systemDefault());
  }

  private static LocalTime toLocalTime(String value) {
    return toLocalDateTime(value).toLocalTime();
  }

  private static AttributeResponse toAttributeResponse(String code, Attribute attribute) {
    return new AttributeResponse(code, singletonList(attribute));
  }

  public static Attribute convertToAttribute(Object obj, BaseField baseField) {
    FieldType fieldType = baseField.getFieldType();

    if (fieldType == FieldType.BOOLEAN) {
      if (obj == null) {
        return new BooleanAttribute(null);
      }
      if (obj instanceof Boolean) {
        return new BooleanAttribute((Boolean) obj);
      }
      if (obj instanceof String) {
        return new BooleanAttribute(Boolean.parseBoolean((String) obj));
      }
      throw new UnprocessableException("Failed to convert to attribute field type " + fieldType);
    } else if (fieldType == FieldType.NUMERIC) {
      if (obj == null) {
        return new NumericAttribute(null);
      }
      if (obj instanceof Double) {
        return new NumericAttribute((Double) obj);
      }
      if (obj instanceof Integer) {
        return new NumericAttribute(((Integer) obj).doubleValue());
      }
      if (obj instanceof String) {
        return new NumericAttribute(Double.parseDouble((String) obj));
      }
      throw new UnprocessableException("Failed to convert to attribute field type " + fieldType);
    } else if (fieldType == FieldType.DATE) {
      if (obj == null) {
        return new DateAttribute(null);
      }
      if (obj instanceof LocalDate) {
        return new DateAttribute((LocalDate) obj);
      }
      if (obj instanceof String) {
        return new DateAttribute(LocalDate.parse((String) obj));
      }
      throw new UnprocessableException("Failed to convert to attribute field type " + fieldType);
    } else if (fieldType == FieldType.DATE_TIME) {
      if (obj == null) {
        return new DateTimeAttribute(null);
      }
      if (obj instanceof LocalDateTime) {
        return new DateTimeAttribute((LocalDateTime) obj);
      }
      if (obj instanceof String) {
        return new DateTimeAttribute(LocalDateTime.parse((String) obj));
      }
      throw new UnprocessableException("Failed to convert to attribute field type " + fieldType);
    } else if (fieldType == FieldType.TIME) {
      if (obj == null) {
        return new TimeAttribute(null);
      }
      if (obj instanceof LocalTime) {
        return new TimeAttribute((LocalTime) obj);
      }
      if (obj instanceof String) {
        return new TimeAttribute(LocalTime.parse((String) obj));
      }
      throw new UnprocessableException("Failed to convert to attribute field type " + fieldType);
    } else if (fieldType == FieldType.STRING) {
      if (obj == null) {
        return new StringAttribute(null);
      }
      return new StringAttribute((String) obj);
    } else if (fieldType == FieldType.RELATION) {
      if (obj == null) {
        return new RelationAttribute(null);
      }
      if (obj instanceof EntityObject) {
        return new RelationAttribute((EntityObject) obj);
      }
      if (obj instanceof String) {
        EntityObject entityobject = EntityUtils.readFromJson((String) obj);
        entityobject.setEntityType(((RelationField) baseField).getRelates());
        return new RelationAttribute(entityobject);
      }
      if (obj instanceof LinkedHashMap) {
        EntityObject entityobject = new EntityObject();
        entityobject.setEntityType((String) ((LinkedHashMap) obj).get("entityType"));
        entityobject.setId((Integer) ((LinkedHashMap) obj).get("id"));
        return new RelationAttribute(entityobject);
      }
      throw new UnprocessableException("Failed to convert to attribute field type " + fieldType);
    } else {
      throw new IllegalArgumentException("unsupported field type " + fieldType);
    }
  }
}

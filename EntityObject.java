package com.example.core.objects.entities;

import static com.example.core.model.entities.Field.FieldValueType.BASE_FIELD;
import static com.example.core.model.entities.Field.FieldValueType.STANDARD_FIELD;

import com.example.core.common.Metadata;
import com.example.core.model.entities.Field;
import com.example.core.model.entities.StandardField;
import com.example.core.objects.api.dto.ObjectAttachment;
import com.example.core.objects.entities.attributes.Attribute;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import org.apache.commons.collections4.map.CaseInsensitiveMap;

@Getter
@Setter
@NoArgsConstructor
@ToString(of = {"id", "entityType", "name", "guid",})
@EqualsAndHashCode(of = {"id", "entityType"})
public class EntityObject {

  private int id;

  private UUID guid;

  private String entityType;

  private String name;

  private Metadata metadata = new Metadata();

  private EntityObjectStatus status = EntityObjectStatus.ACTIVE;

  @Setter(AccessLevel.NONE)
  private Map<String, List<Attribute>> attributes = new CaseInsensitiveMap<>();

  private Integer parentId;

  @Setter(AccessLevel.NONE)
  private List<ObjectAttachment> attachments = new ArrayList<>();

  private boolean checkRule = true;

  public EntityObject(int id, String entityType, String name) {
    this.id = id;
    this.entityType = entityType;
    this.name = name;
  }

  public void add(String field, Attribute attribute) {
    List<Attribute> values = attributes.computeIfAbsent(field, k -> new ArrayList<>());
    values.add(attribute);
  }

  public void add(String field, Attribute... attributes) {
    this.attributes.computeIfAbsent(field, k -> new ArrayList<>());
    Arrays.stream(attributes).forEach(attribute -> add(field, attribute));
  }

  public void add(String field, List<Attribute> attributes) {
    this.attributes.computeIfAbsent(field, k -> new ArrayList<>());
    attributes.forEach(attribute -> add(field, attribute));
  }

  public void setAttributes(Map<String, List<Attribute>> attributes) {
    this.attributes.clear();
    this.attributes.putAll(attributes);
  }

  public void set(String field, Attribute attribute) {
    this.attributes.remove(field);
    add(field, attribute);
  }

  public Optional<List<Attribute>> get(String field) {
    return Optional.ofNullable(attributes.get(field));
  }

  public Optional<Attribute> getSingle(String field) {
    List<Attribute> attributes = this.attributes.get(field);
    return attributes == null ? Optional.empty() :
        attributes.stream().findFirst();
  }

  public Optional<Object> getSingleValue(Field field) {
    Field.FieldValueType valueType = field.getValueType();
    if (valueType == STANDARD_FIELD) {
      return Optional.ofNullable(getStandardFieldValue((StandardField) field));
    } else if (valueType == BASE_FIELD) {
      Optional<Attribute> attribute = getSingle(field.getCodeName());
      return attribute.map(Attribute::getValue);
    }
    return Optional.empty();
  }

  /**
   * Получить значение стандартного поля объекта
   *
   * @param field стандратное поле
   * @return значение поля
   */
  public Object getStandardFieldValue(StandardField field) {
    if (field == StandardField.ID) {
      return getId();
    } else if (field == StandardField.STATUS) {
      return getStatus().ordinal();
    } else if (field == StandardField.NAME) {
      return getName();
    } else if (field == StandardField.PARENT_ID) {
      return getParentId();
    } else if (field == StandardField.CREATE_USER) {
      return getMetadata() == null ? null : getMetadata().getCreateUser();
    } else if (field == StandardField.CREATE_DATE) {
      return getMetadata() == null ? null : getMetadata().getCreateDate();
    } else if (field == StandardField.CHANGE_USER) {
      return getMetadata() == null ? null : getMetadata().getChangeUser();
    } else if (field == StandardField.CHANGE_DATE) {
      return getMetadata() == null ? null : getMetadata().getChangeDate();
    } else if (field == StandardField.GUID) {
      return getGuid();
    } else if (field == StandardField.ATTACHMENTS) {
      return getAttachments();
    } else {
      throw new IllegalArgumentException("invalid standard field");
    }
  }

  /**
   * Назначить значение стандартного поля объекта
   *
   * @param field стандратное поле
   * @param value значение
   */
  public void setStandardFieldValue(StandardField field, Object value) {
    if (field == StandardField.ID) {
      int id = value == null ? 0 : (int) value;
      setId(id);
    } else if (field == StandardField.STATUS) {
      int status = value == null ? EntityObjectStatus.ACTIVE.ordinal() : (int) value;
      setStatus(EntityObjectStatus.values()[status]);
    } else if (field == StandardField.NAME) {
      setName(value == null ? null : String.valueOf(value));
    } else if (field == StandardField.PARENT_ID) {
      setParentId((Integer) value);
    } else if (field == StandardField.CREATE_USER) {
      getMetadata().setCreateUser((String) value);
    } else if (field == StandardField.CREATE_DATE) {
      getMetadata().setCreateDate((LocalDateTime) value);
    } else if (field == StandardField.CHANGE_USER) {
      getMetadata().setChangeUser((String) value);
    } else if (field == StandardField.CHANGE_DATE) {
      getMetadata().setChangeDate((LocalDateTime) value);
    } else if (field == StandardField.GUID) {
      setGuid((UUID) value);
    } else if (field == StandardField.ATTACHMENTS) {
      setAttachments((List<ObjectAttachment>) value);
    } else {
      throw new IllegalArgumentException("invalid standard field");
    }
  }

  /**
   * Узнать новый ли объект
   */
  public boolean isNew() {
    return id <= 0;
  }

  public void setAttachments(List<ObjectAttachment> attachments) {
    this.attachments = attachments == null ? new ArrayList<>() : attachments;
  }
}

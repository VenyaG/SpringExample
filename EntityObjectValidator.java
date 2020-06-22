package com.example.core.objects;

import com.example.common.validation.Validated;
import com.example.core.model.EntityUtils;
import com.example.core.model.entities.EntityType;
import com.example.core.model.entities.fields.BaseField;
import com.example.core.objects.entities.EntityObject;
import java.util.Map;
import lombok.NonNull;
import org.springframework.stereotype.Component;
import org.springframework.validation.BindException;
import org.springframework.validation.Errors;

/**
 * Класс для валидации объектов
 */
@Component
public class EntityObjectValidator {

  /**
   * Провести проверку объекта.
   *
   * Если объект новый ({@link EntityObject#getId()}  <= 0}) проверяется наличие всех обязателельных
   * полей.
   *
   * Иначе идет проверка только полей, представленных в {@link EntityObject#getAttributes()}
   *
   * @param entityType Класс объектов
   * @param object объект
   * @param objectName имя объекта
   * @return найденные ошибки
   */
  public Validated validate(EntityType entityType, EntityObject object,
                            @NonNull String objectName) {
    BindException errors = new BindException(object, objectName);
    validate(entityType, object, errors);
    return Validated.of(errors);
  }

  /**
   * Провести проверку объекта.
   *
   * Если объект новый ({@link EntityObject#getId()}  <= 0}) проверяется наличие всех обязателельных
   * полей.
   *
   * Иначе идет проверка только полей, представленных в {@link EntityObject#getAttributes()}
   *
   * @param entityType Класс объектов
   * @param object объект
   * @param errors состояние процесса проверки
   */
  public void validate(@NonNull EntityType entityType,
                       @NonNull EntityObject object,
                       @NonNull Errors errors) {
    Map<BaseField, Object> valueMap = EntityUtils.attributeValueMap(entityType, object);
    if (object.isNew()) {
      entityType.getFields().forEach(field -> validateField(field, valueMap.get(field), errors));
    } else {
      valueMap.forEach((f, v) -> validateField(f, v, errors));
    }
  }

  private void validateField(BaseField field, Object value, Errors errors) {
    if (field.isMultiple()) {
      validateMultipleField(field, (Object[]) value, errors);
    } else {
      validateSingleField(field, value, errors);
    }
  }

  private void validateSingleField(BaseField field, Object value, Errors errors) {
    if (value == null) {
      if (field.isRequired()) {
        rejectField(errors, "entity.object.field.is.null", field.getName());
      }
    }
  }

  private void validateMultipleField(BaseField field, Object[] values, Errors errors) {
    if (values == null || values.length == 0) {
      if (field.isRequired()) {
        rejectField(errors, "entity.object.multiple.field.is.empty", field.getName());
      }
    }
  }

  private void rejectField(Errors errors, String code, Object... args) {
    errors.reject(code, args, null);
  }

}

package com.example.core.objects;

import static com.example.core.common.storage.StorageBuckets.ATTACHMENTS_BUCKET;
import static com.example.core.model.EntityUtils.standardFields;
import static com.example.core.objects.entities.EntityObjectStatus.ACTIVE;
import static com.example.core.objects.entities.EntityObjectStatus.INACTIVE;
import static com.example.core.registers.entities.Register.ADMIN_CODE_NAME;

import com.example.common.context.RequestContext;
import com.example.core.common.Metadata;
import com.example.core.common.Status;
import com.example.core.common.db.query.select.CqlFilterCondition;
import com.example.core.common.storage.StorageBuckets;
import com.example.core.limitations.FilesUpdate;
import com.example.core.limitations.LicenseLimitsValidator;
import com.example.core.limitations.LimitKey;
import com.example.core.limitations.LimitsCounter;
import com.example.core.model.EntityTypeManager;
import com.example.core.model.EntityUtils;
import com.example.core.model.entities.EntityType;
import com.example.core.model.entities.Field;
import com.example.core.model.entities.fields.BaseField;
import com.example.core.model.exceptions.EntityTypeNotFoundException;
import com.example.core.objects.api.dto.ObjectAttachment;
import com.example.core.objects.entities.EntityObject;
import com.example.core.objects.entities.EntityObjectFilter;
import com.example.core.objects.entities.EntityObjectStatus;
import com.example.core.objects.entities.SearchRecord;
import com.example.core.objects.exceptions.ObjectAttachmentAlreadyExistsException;
import com.example.core.objects.exceptions.ObjectAttachmentNotFoundException;
import com.example.core.objects.exceptions.ObjectIsActiveException;
import com.example.core.objects.exceptions.ObjectNotFoundException;
import com.example.core.objects.repository.EntityObjectRepository;
import com.example.core.scripting.rules.RestrictiveRuleChecker;
import com.example.storage.api.StorageFile;
import com.example.storage.api.StorageService;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.NonNull;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Менеджер объектов {@link EntityObject}
 */
@Service
@Transactional(rollbackFor = Exception.class)
public class EntityObjectManager {

  private final EntityObjectRepository repository;

  private final EntityTypeManager etMan;

  private final EntityObjectValidator validator;

  private final RestrictiveRuleChecker ruleChecker;

  private final StorageService storageService;

  private final EntitySelectBuilderFactory selectBuilderFactory;

  private final LicenseLimitsValidator limitsValidator;

  private final LimitsCounter counter;

  @Autowired
  public EntityObjectManager(EntityObjectRepository repository,
                             EntityTypeManager etMan,
                             EntityObjectValidator validator,
                             RestrictiveRuleChecker ruleChecker,
                             StorageService storageService,
                             EntitySelectBuilderFactory selectBuilderFactory,
                             LicenseLimitsValidator limitsValidator,
                             LimitsCounter counter) {
    this.repository = repository;
    this.etMan = etMan;
    this.validator = validator;
    this.ruleChecker = ruleChecker;
    this.storageService = storageService;
    this.selectBuilderFactory = selectBuilderFactory;
    this.limitsValidator = limitsValidator;
    this.counter = counter;
  }

  /**
   * Поиск объектов с фильтром
   *
   * @param register кодовое имя сервиса
   * @param entityType класс объектов
   * @param filter фильтр
   * @return найденные объекты
   */
  public Page<EntityObject> findObjectsWithFilter(String register, EntityType entityType,
                                                  EntityObjectFilter filter) {
    EntitySelectBuilder query = buildQueryWithFilter(register, entityType, filter);
    query.withFields(EntityUtils.standardFields(entityType))
        .pageable(new PageRequest(filter.getPage(), filter.getPageSize()));
    return repository.findAll(query);
  }

  /**
   * Получить список объектов. Пагинация игнорируется
   *
   * @param register кодовое имя сервиса
   * @param codeName кодовое имя класса объектов
   */
  public List<EntityObject> findAll(String register, String codeName, EntityObjectFilter filter) {
    EntityType entityType = etMan.find(register, codeName);
    EntitySelectBuilder query = buildQueryWithFilter(register, entityType, filter)
        .withFields(EntityUtils.standardFields(entityType));
    return repository.findAll(query).getContent();
  }

  /**
   * Получить список объектов. Количество объектов задается фильтром
   *
   * @param entityType класс объектов
   * @param filter фильтр
   * @return список объектов
   */
  public List<EntityObject> findAll(EntityType entityType, EntityObjectFilter filter) {
    EntitySelectBuilder query = buildQueryWithFilterAndPageable(ADMIN_CODE_NAME, entityType, filter)
        .withFields(EntityUtils.standardFields(entityType));
    return repository.findAll(query).getContent();
  }

  /**
   * Получить количество объектов
   *
   * @param register кодовое имя сервиса
   * @param codeName кодовое имя класса объектов
   * @param filter фильтр
   * @return количество объектов
   */
  public int count(String register, String codeName, EntityObjectFilter filter) {
    EntityType entityType = etMan.find(register, codeName);
    EntitySelectBuilder query = buildQueryWithFilter(register, entityType, filter);
    return repository.count(query);
  }

  /**
   * Поиск записей с возможностью аггрегации
   *
   * @param register кодовое имя сервиса
   * @param codeName кодовое имя класса объектов
   * @param filter фильтр
   * @return найденные записи
   */
  public Page<SearchRecord> findRecords(String register, String codeName,
                                        EntityObjectFilter filter) {
    // TODO Отрефакторить, вынести поиск в отдельный класс
    EntityType entityType = etMan.find(register, codeName);
    EntitySelectBuilder query = buildQueryWithFilter(register, entityType, filter)
        .withFields(EntityUtils.standardFields(entityType))
        .pageable(new PageRequest(filter.getPage(), filter.getPageSize()))
        .aggregate(filter.getAggregates());
    return repository.findRecords(query);
  }


  /**
   * Найти все уникальные значения по полю
   *
   * @param register кодовое имя сервиса
   * @param entityType класс объектов
   * @param filter поле для поиска
   * @return список уникальных значений
   */
  public List<String> findFilterValues(String register, EntityType entityType, String filter) {
    return repository.findUniqueValues(entityType, filter);
  }

  /**
   * Найти объект
   *
   * @param register кодовое имя сервиса
   * @param codeName кодовое имя класса объектов
   * @param objectId идентификатор объекта
   * @return найденный объект
   * @throws EntityTypeNotFoundException класс объектов не найден
   * @throws ObjectNotFoundException объект не найден
   */
  public ObjectResponse find(String register, String codeName, int objectId) {
    EntityType entityType = etMan.find(register, codeName);
    return find(register, entityType, objectId, entityType.getFields());
  }

  /**
   * Найти объект
   *
   * @param register кодовое имя сервиса
   * @param codeName кодовое имя класса объектов
   * @param objectId идентификатор объекта
   * @param fields кодовые имена полей, которые нужно получить вместе с объектом
   * @return найденный объект
   * @throws EntityTypeNotFoundException класс объектов не найден
   * @throws ObjectNotFoundException объект не найден
   */
  public ObjectResponse find(String register, String codeName, int objectId, List<String> fields) {
    EntityType entityType = etMan.find(register, codeName);
    Map<String, BaseField> fieldMap = entityType.fieldMap();
    List<BaseField> objectFields = fields.stream().map(fieldMap::get).filter(Objects::nonNull).
        collect(Collectors.toList());
    return find(register, entityType, objectId, objectFields);
  }

  /**
   * Найти объект
   *
   * @param register кодовое имя сервиса
   * @param codeName кодовое имя класса объектов
   * @param guid глобальный идентификатор объекта
   * @return найденный объект
   * @throws EntityTypeNotFoundException класс объектов не найден
   * @throws ObjectNotFoundException объект не найден
   */
  public ObjectResponse find(String register, String codeName, UUID guid) {
    EntityType entityType = etMan.find(register, codeName);

    EntityObject object = repository.findOne(entityType, guid)
        .orElseThrow(ObjectNotFoundException::new);
    return ObjectResponse.of(entityType, object);
  }

  /**
   * Найти объект
   *
   * @param entityType класс объектов
   * @param objectId идентификатор объекта
   * @return найденный объект
   * @throws EntityTypeNotFoundException класс объектов не найден
   * @throws ObjectNotFoundException объект не найден
   */
  public ObjectResponse find(EntityType entityType, int objectId, int crs) {
    EntityObject object = repository.findOne(entityType, objectId, crs)
        .orElseThrow(ObjectNotFoundException::new);
    return ObjectResponse.of(entityType, object);
  }

  /**
   * Получить файл из объекта
   *
   * @param response ответ, содержащий в себе объект
   * @param guid идентификатор файла объекта
   */
  public File getObjectAttachmentFile(@NonNull ObjectResponse response, String guid) {
    if (response.getObject().getAttachments().stream().noneMatch(
        o -> o.getGuid().equals(guid))) {
      throw new ObjectAttachmentNotFoundException();
    }
    return storageService.extractFile(StorageBuckets.ATTACHMENTS_BUCKET, guid).toFile();
  }

  /**
   * Получить метадату файла из бакета attachments
   *
   * @param guid идентификатор файла объекта
   */
  public StorageFile getAttachmentFileMetadata(String guid) {
    return storageService.getFileMetadata(StorageBuckets.ATTACHMENTS_BUCKET, guid);
  }

  /**
   * Получить информацию о файле из объекта
   *
   * @param response ответ, содержащий в себе объект
   * @param guid идентификатор файла объекта
   */
  public ObjectAttachment findObjectAttachmentInfo(@NonNull ObjectResponse response, String guid) {
    return response.getObject().getAttachments().stream().filter(
        o -> o.getGuid().equals(guid)).findAny()
        .orElseThrow(ObjectAttachmentNotFoundException::new);
  }

  /**
   * Получить список файлов объекта
   *
   * @param response ответ, содержащий в себе объект
   */
  public List<ObjectAttachment> getObjectAttachments(@NonNull ObjectResponse response) {
    return response.getObject().getAttachments();
  }

  /**
   * Создать новый объект
   *
   * @param register кодовое имя сервиса
   * @param codeName кодовое имя класса объектов
   * @param object объект
   * @throws EntityTypeNotFoundException класс объектов не найден
   */
  public EntityObject createObject(String register, String codeName, @NonNull EntityObject object) {
    EntityType entityType = etMan.find(register, codeName);
    return createObject(entityType, object);
  }

  /**
   * Создать новый объект
   *
   * @param entityType класс объектов
   * @param object объект
   */
  public EntityObject createObject(EntityType entityType, @NonNull EntityObject object) {
    limitsValidator.checkLimit(LimitKey.OBJECTS);
    object.setId(0);
    object.setGuid(null);
    object.setParentId(null);
    object.setMetadata(new Metadata(RequestContext.getUser()));
    object.setStatus(ACTIVE);

    FilesUpdate filesUpdate = updateAttachments(object, null);

    EntityObject created = validateAndSave(entityType, object);
    counter.inc(LimitKey.OBJECTS);
    counter.updateCount(LimitKey.FILES, filesUpdate.getCount());
    counter.updateCount(LimitKey.FILES_AMOUNT, filesUpdate.getSize());
    return created;
  }

  /**
   * Изменить существующий объект
   *
   * @param register кодовое имя сервиса
   * @param codeName кодовое имя класса объектов
   * @param object объект
   * @throws EntityTypeNotFoundException класс объектов не найден
   * @throws ObjectNotFoundException объект не найден
   */
  public void updateObject(String register, String codeName, @NonNull EntityObject object) {
    EntityType entityType = etMan.find(register, codeName);
    updateObject(entityType, object);
  }

  /**
   * Изменить существующий объект
   *
   * @param entityType класс объектов
   * @param object объект
   * @throws EntityTypeNotFoundException класс объектов не найден
   * @throws ObjectNotFoundException объект не найден
   */
  public void updateObject(EntityType entityType, @NonNull EntityObject object) {
    int objectId = object.getId();
    EntityObject original = repository.findOne(entityType, objectId)
        .orElseThrow(ObjectNotFoundException::new);
    // Перетаскиваем только те атрибуты, которые могут меняться при редактировании
    original.getMetadata().changed(RequestContext.getUser());
    original.setName(object.getName());
    original.setAttributes(object.getAttributes());
    FilesUpdate filesUpdate = updateAttachments(object, original);
    original.setCheckRule(object.isCheckRule());
    validateAndSave(entityType, original);

    counter.updateCount(LimitKey.FILES, filesUpdate.getCount());
    counter.updateCount(LimitKey.FILES_AMOUNT, filesUpdate.getSize());
  }

  private FilesUpdate updateAttachments(EntityObject object, EntityObject original) {
    if (object.getAttachments() == null) {
      return FilesUpdate.of(0, 0);
    }
    FilesUpdate filesUpdate = new FilesUpdate();
    object.getAttachments().forEach(a -> {
      if (a.getStatus() == Status.CREATE) {
        filesUpdate.incCount();
        filesUpdate.incSize(a.getSize());
      } else if (a.getStatus() == Status.DELETE) {
        filesUpdate.decCount();
        filesUpdate.decSize(a.getSize());
      }
    });
    limitsValidator.checkLimit(LimitKey.FILES, filesUpdate.getCount());
    limitsValidator.checkLimit(LimitKey.FILES_AMOUNT, filesUpdate.getSize());
    for (ObjectAttachment objectAttachment : object.getAttachments()) {
      switch (objectAttachment.getStatus()) {
        case CREATE:
          addAttachmentObject(objectAttachment, original);
          break;
        case DELETE:
          deleteAttachmentObject(objectAttachment, original);
          break;
      }
    }
    return filesUpdate;
  }

  private FilesUpdate clearAttachments(EntityObject object) {
    List<ObjectAttachment> attachments = new ArrayList<>(object.getAttachments());
    FilesUpdate filesUpdate = new FilesUpdate();
    attachments.stream().peek(a -> {
      filesUpdate.decCount();
      filesUpdate.decSize(a.getSize());
    }).forEach(a -> storageService.deleteFile(ATTACHMENTS_BUCKET, a.getGuid()));
    return filesUpdate;
  }

  private void addAttachmentObject(ObjectAttachment objectAttachment,
                                   EntityObject original) {
    objectAttachment.setCreateUser(RequestContext.getUser());

    if (original == null) {
      prepareAttachmentObject(objectAttachment);
    } else if (original.getAttachments().stream()
        .noneMatch(o -> o.getGuid().equals(objectAttachment.getGuid()))) {

      prepareAttachmentObject(objectAttachment);
      original.getAttachments().add(objectAttachment);
    } else {
      throw new ObjectAttachmentAlreadyExistsException();
    }
  }

  private void prepareAttachmentObject(ObjectAttachment objectAttachment) {
    storageService.makeFilePermanent(objectAttachment.getGuid(), ATTACHMENTS_BUCKET, false);

    StorageFile metadata = storageService
        .getFileMetadata(ATTACHMENTS_BUCKET, objectAttachment.getGuid());
    objectAttachment.setName(metadata.getName());
    objectAttachment.setMd5(metadata.getMd5());
    objectAttachment.setSize(metadata.getSize());
  }

  private void deleteAttachmentObject(ObjectAttachment objectAttachment, EntityObject original) {
    original.getAttachments().remove(original.getAttachments().stream().filter(
        o -> o.getGuid().equals(objectAttachment.getGuid())).findAny()
                                         .orElseThrow(ObjectAttachmentNotFoundException::new));

    storageService.deleteFile(ATTACHMENTS_BUCKET, objectAttachment.getGuid());
  }

  /**
   * Сделать объект активным
   *
   * @param register кодовое имя сервиса
   * @param codeName кодовое имя класса объектов
   * @param objectId идентификатор объекта
   * @throws EntityTypeNotFoundException класс объектов не найден
   * @throws ObjectNotFoundException объект не найден
   * @throws ObjectIsActiveException объект уже активен
   */
  public void activateObject(String register, String codeName, int objectId) {
    EntityType entityType = etMan.find(register, codeName);
    EntityObject object = repository.findOneBase(entityType, objectId)
        .orElseThrow(ObjectNotFoundException::new);
    if (object.getStatus() == ACTIVE) {
      throw new ObjectIsActiveException();
    }
    object.setStatus(ACTIVE);
    object.getMetadata().changed(RequestContext.getUser());
    repository.save(entityType, object);
  }

  /**
   * Сделать объект неактивным или удалить его
   *
   * @param register кодовое имя сервиса
   * @param codeName кодовое имя класса объектов
   * @param objectId идентификатор объекта
   * @throws EntityTypeNotFoundException класс объектов не найден
   * @throws ObjectNotFoundException объект не найден
   */
  public void deleteObject(String register, String codeName, int objectId) {
    EntityType entityType = etMan.find(register, codeName);
    EntityObject object = repository.findOneBase(entityType, objectId)
        .orElseThrow(ObjectNotFoundException::new);
    EntityObjectStatus status = object.getStatus();
    if (status == ACTIVE) {
      object.setStatus(INACTIVE);
      object.getMetadata().changed(RequestContext.getUser());
      repository.save(entityType, object);
    } else {
      deleteObjectForce(entityType, object);
    }
  }

  /**
   * Удалить объект если он есть.
   *
   * @param register кодовое имя сервиса
   * @param codeName кодовое имя класса объектов
   * @param objectId идентификатор объекта
   */
  public void deleteObjectForce(String register, String codeName, int objectId) {
    EntityType entityType = etMan.find(register, codeName);
    repository.findOneBase(entityType, objectId)
        .ifPresent(object -> deleteObjectForce(entityType, object));
  }

  private void deleteObjectForce(EntityType entityType, EntityObject object) {
    repository.delete(entityType, object);

    FilesUpdate filesUpdate = clearAttachments(object);
    counter.dec(LimitKey.OBJECTS);
    counter.updateCount(LimitKey.FILES, filesUpdate.getCount());
    counter.updateCount(LimitKey.FILES_AMOUNT, filesUpdate.getSize());
  }

  private ObjectResponse find(String register, EntityType entityType, int objectId,
                              List<BaseField> fields) {
    EntitySelectBuilder query = selectBuilderFactory.newBuilder(register, entityType)
        .withFields(standardFields(entityType))
        .withFields(fields.toArray(new Field[0]))
        .withId(objectId);
    return repository.findOne(query)
        .map(o -> ObjectResponse.of(entityType, o))
        .orElseThrow(ObjectNotFoundException::new);
  }

  private EntityObject validateAndSave(EntityType entityType, EntityObject object) {
    validator.validate(entityType, object, entityType.getCodeName()).requireValid();
    if (object.isCheckRule()) {
      ruleChecker.check(entityType, object).requireValid();
    }
    return repository.save(entityType, object);
  }

  private EntitySelectBuilder buildQueryWithFilter(String register, EntityType entityType,
                                                   EntityObjectFilter filter) {
    Set<Field> fields = filter.getFields().stream().map(entityType::getField)
        .filter(Optional::isPresent)
        .map(Optional::get)
        .collect(Collectors.toSet());

    EntitySelectBuilder builder = selectBuilderFactory.newBuilder(register, entityType)
        .withFields(fields)
        .srid(filter.getSrid());

    entityType.getField(filter.getSortField())
        .ifPresent(field -> builder.sort(field, filter.getSortType()));

    if (StringUtils.isNotBlank(filter.getCql())) {
      CqlFilterCondition cql = CqlFilterCondition.builder()
          .from(entityType).where(filter.getCql()).build();
      builder.where(cql);
    }
    return builder;
  }

  private EntitySelectBuilder buildQueryWithFilterAndPageable(String register,
                                                              EntityType entityType,
                                                              EntityObjectFilter filter) {
    Set<Field> fields = filter.getFields().stream().map(entityType::getField)
        .filter(Optional::isPresent)
        .map(Optional::get)
        .collect(Collectors.toSet());

    EntitySelectBuilder builder = selectBuilderFactory.newBuilder(register, entityType)
        .withFields(fields);

    entityType.getField(filter.getSortField())
        .ifPresent(field -> builder.sort(field, filter.getSortType()));

    if (StringUtils.isNotBlank(filter.getCql())) {
      CqlFilterCondition cql = CqlFilterCondition.builder()
          .from(entityType).where(filter.getCql()).build();
      builder.where(cql);
    }

    Pageable pageable = new PageRequest(filter.getPage(), filter.getPageSize());
    builder.pageable(pageable);

    return builder;
  }

}

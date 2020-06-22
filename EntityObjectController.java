package com.example.core.objects.api;

import static com.example.common.rest.ApiUtils.JSON_TYPE;

import com.example.core.common.Filter;
import com.example.common.rest.CountResponse;
import com.example.core.model.EntityTypeManager;
import com.example.core.model.entities.EntityType;
import com.example.core.objects.EntityObjectManager;
import com.example.core.objects.ObjectResponse;
import com.example.core.objects.api.dto.EntityObjectDTO;
import com.example.core.objects.api.dto.EntityReference;
import com.example.core.objects.api.dto.ObjectAttachment;
import com.example.core.objects.calculator.EntityCalculatorManager;
import com.example.core.objects.calculator.log.CalculatorTaskManager;
import com.example.core.objects.calculator.log.entity.CalculatorTask;
import com.example.core.objects.calculator.log.entity.CalculatorTaskLog;
import com.example.core.objects.calculator.log.entity.CalculatorTaskLogFilter;
import com.example.core.objects.entities.EntityObject;
import com.example.core.objects.entities.EntityObjectFilter;
import com.example.core.objects.entities.SearchRecord;
import com.example.storage.api.StorageFile;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.InputStreamResource;
import org.springframework.core.io.Resource;
import org.springframework.data.domain.Page;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

/**
 * Контроллер для объектов {@link com.example.core.objects.entities.EntityObject}
 */
@RestController
@RequestMapping(path = "/registers/{register}/model/{entityType}/objects", produces = JSON_TYPE)
public class EntityObjectController {

  private final EntityObjectManager manager;

  private final EntityTypeManager etManager;

  private final EntityCalculatorManager calcManager;

  private final CalculatorTaskManager taskManager;

  @Autowired
  public EntityObjectController(EntityObjectManager manager, EntityTypeManager etManager,
                                EntityCalculatorManager calcManager,
                                CalculatorTaskManager taskManager) {
    this.manager = manager;
    this.etManager = etManager;
    this.calcManager = calcManager;
    this.taskManager = taskManager;
  }

  @GetMapping
  public Page<EntityObjectDTO> findObjectsWithFilter(@PathVariable("register") String register,
                                                     @PathVariable("entityType") String entityType,
                                                     EntityObjectFilter filter) {
    EntityType type = etManager.find(register, entityType);
    Page<EntityObject> objects = manager.findObjectsWithFilter(register, type, filter);
    if (filter.getCalculateAttribute() != null && filter.getCalculateAttribute().length != 0) {
      objects = calcManager.calculate(objects, type, filter);
    }
    return objects.map(object -> EntityObjectMapper.map(type, object));
  }

  @GetMapping("count")
  public CountResponse countObjects(@PathVariable("register") String register,
                                    @PathVariable("entityType") String entityType,
                                    EntityObjectFilter filter) {
    return CountResponse.of(manager.count(register, entityType, filter));
  }

  @GetMapping("{guid:[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}}")
  public EntityObjectDTO findWithGuid(@PathVariable("register") String register,
                                      @PathVariable("entityType") String entityType,
                                      @PathVariable("guid") UUID guid) {
    ObjectResponse response = manager.find(register, entityType, guid);
    return EntityObjectMapper.map(response.getEntityType(), response.getObject());
  }

  @GetMapping("{id:\\d+}")
  public EntityObjectDTO find(@PathVariable("register") String register,
                              @PathVariable("entityType") String entityType,
                              @PathVariable("id") int id) {
    ObjectResponse response = manager.find(register, entityType, id);
    return EntityObjectMapper.map(response.getEntityType(), response.getObject());
  }

  @GetMapping("/param")
  public List<String> findFilterValues(@PathVariable("register") String register,
                                       @PathVariable("entityType") String entityType,
                                       @RequestParam("filter") String filter) {
    EntityType type = etManager.find(register, entityType);
    return manager.findFilterValues(register, type, filter);
  }

  @GetMapping("{id}/attachments/{guid}")
  public ResponseEntity<Resource> findObjectAttachment(@PathVariable("register") String register,
                                                       @PathVariable("entityType") String codeName,
                                                       @PathVariable("id") int id,
                                                       @PathVariable("guid") String guid)
      throws IOException {
    ObjectResponse response = manager.find(register, codeName, id);
    File file = manager.getObjectAttachmentFile(response, guid);
    StorageFile storageFile = manager.getAttachmentFileMetadata(guid);

    InputStreamResource resource = new InputStreamResource(new FileInputStream(file));
    return ResponseEntity.ok()
        .contentLength(file.length())
        .contentType(MediaType.parseMediaType(storageFile.getContentType()))
        .header("Content-Disposition", "attachment; filename=\"" + storageFile.getName() + "\"")
        .body(resource);
  }

  @GetMapping("{id}/attachments/{guid}/info")
  public ObjectAttachment findObjectAttachmentInfo(@PathVariable("register") String register,
                                                   @PathVariable("entityType") String codeName,
                                                   @PathVariable("id") int id,
                                                   @PathVariable("guid") String guid) {
    ObjectResponse response = manager.find(register, codeName, id);
    return manager.findObjectAttachmentInfo(response, guid);
  }

  @GetMapping("{id}/attachments")
  public List<ObjectAttachment> objectAttachments(@PathVariable("register") String register,
                                                  @PathVariable("entityType") String codeName,
                                                  @PathVariable("id") int id) {
    ObjectResponse response = manager.find(register, codeName, id);
    return manager.getObjectAttachments(response);

  }

  @PostMapping("/search")
  public Page<SearchRecord> findRecordsWithFilter(@PathVariable("register") String register,
                                                  @PathVariable("entityType") String entityType,
                                                  @RequestBody EntityObjectFilter filter) {
    return manager.findRecords(register, entityType, filter);
  }

  @PostMapping("/filter")
  public Page<EntityObjectDTO> findObjectsWithFilterPost(@PathVariable("register") String register,
                                                         @PathVariable("entityType") String entityType,
                                                         @RequestBody EntityObjectFilter filter) {
    EntityType type = etManager.find(register, entityType);
    Page<EntityObject> objects = manager.findObjectsWithFilter(register, type, filter);
    return objects.map(object -> EntityObjectMapper.map(type, object));
  }

  @PostMapping
  @ResponseStatus(HttpStatus.CREATED)
  public EntityReference create(@PathVariable("register") String register,
                                @PathVariable("entityType") String codeName,
                                @RequestBody String json) {
    EntityType entityType = etManager.find(register, codeName);
    EntityObject object = EntityObjectMapper.mapFromJson(entityType, json);
    EntityObject saved = manager.createObject(register, codeName, object);
    return EntityObjectMapper.mapReference(saved);
  }

  @PatchMapping("{id}")
  @ResponseStatus(HttpStatus.NO_CONTENT)
  public void update(@PathVariable("register") String register,
                     @PathVariable("entityType") String codeName,
                     @PathVariable("id") int id,
                     @RequestBody String json) {
    EntityType entityType = etManager.find(register, codeName);
    EntityObject object = EntityObjectMapper.mapFromJson(entityType, json);
    object.setId(id);
    manager.updateObject(register, codeName, object);
  }

  @DeleteMapping("{id}")
  @ResponseStatus(HttpStatus.NO_CONTENT)
  public void delete(@PathVariable("register") String register,
                     @PathVariable("entityType") String codeName,
                     @PathVariable("id") int id) {
    manager.deleteObject(register, codeName, id);
  }

  @PatchMapping("{id}/activate")
  @ResponseStatus(HttpStatus.NO_CONTENT)
  public void activate(@PathVariable("register") String register,
                       @PathVariable("entityType") String codeName,
                       @PathVariable("id") int id) {
    manager.activateObject(register, codeName, id);
  }

  @PatchMapping("calculate/save")
  @ResponseStatus(HttpStatus.NO_CONTENT)
  public Map<String, Long> calculateAndSave(@PathVariable("register") String register,
                                            @PathVariable("entityType") String codeName,
                                            @RequestBody EntityObjectFilter filter) {
    EntityType entityType = etManager.find(register, codeName);
    Map<String, Long> result = new HashMap<>();
    long id = calcManager.calculateAndSave(entityType, filter);
    result.put("taskId", id);
    return result;
  }

  @GetMapping("tasks")
  public Page<CalculatorTask> findTasks(Filter filter) {
    return taskManager.findTasks(filter);
  }

  @GetMapping("tasks/logs")
  public Page<CalculatorTaskLog> findTaskLogs(CalculatorTaskLogFilter filter) {
    return taskManager.findTaskLogs(filter);
  }
}

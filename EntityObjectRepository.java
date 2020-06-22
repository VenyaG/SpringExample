package com.example.core.objects.repository;

import static com.example.core.model.DbNameProvider.entityTypeTable;
import static com.example.core.model.DbNameProvider.fieldDstColumn;
import static com.example.core.model.DbNameProvider.fieldSrcColumn;
import static com.example.core.model.DbNameProvider.relationTable;
import static com.example.core.model.EntityUtils.fullAttributeValueMap;
import static com.example.core.model.EntityUtils.hasRelationTable;
import static com.example.core.model.EntityUtils.innerField;
import static com.example.core.model.EntityUtils.standardFields;
import static com.example.core.model.entities.StandardField.CREATE_DATE;
import static com.example.core.model.entities.StandardField.CREATE_USER;
import static com.example.core.model.entities.StandardField.GUID;
import static com.example.core.model.entities.StandardField.ID;
import static com.example.core.objects.api.EntityObjectMapper.mapToJsonString;
import static java.text.MessageFormat.format;

import com.example.core.common.db.SqlArrayUtils;
import com.example.core.gis.Point;
import com.example.core.layers.entities.Extent;
import com.example.core.model.DbNameProvider;
import com.example.core.model.entities.EntityType;
import com.example.core.model.entities.Field;
import com.example.core.model.entities.FieldType;
import com.example.core.model.entities.fields.BaseField;
import com.example.core.model.entities.fields.GeometryField;
import com.example.core.model.entities.fields.RelationField;
import com.example.core.model.exceptions.FieldNotFoundException;
import com.example.core.objects.EntitySelectBuilder;
import com.example.core.objects.EntitySelectBuilderFactory;
import com.example.core.objects.entities.EntityObject;
import com.example.core.objects.entities.SearchRecord;
import com.example.core.objects.entities.attributes.Geometry;
import com.example.core.objects.entities.attributes.Geometry.Type;
import com.example.core.objects.exceptions.EntityGeometryException;
import java.sql.Array;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.UUID;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.stereotype.Repository;

/**
 * Репизоторий объектов {@link EntityObject}
 */
@Slf4j
@Repository
public class EntityObjectRepository {

  private static final String GEOM_FUNCTION = "ST_SetSRID({0}(?), {1,number,#})";

  private static final String ST_FROM_TEXT = "ST_GeomFromText";

  private static final String ST_FROM_GEOJSON = "ST_GeomFromGeoJSON";

  private final JdbcTemplate jdbcTemplate;

  private final EntitySelectBuilderFactory selectBuilderFactory;

  @Autowired
  public EntityObjectRepository(JdbcTemplate jdbcTemplate,
                                EntitySelectBuilderFactory selectBuilderFactory) {
    this.jdbcTemplate = jdbcTemplate;
    this.selectBuilderFactory = selectBuilderFactory;
  }

  /**
   * Загрузить страницу объектов
   *
   * @param query запрос
   */
  public Page<EntityObject> findAll(EntitySelectBuilder query) {
    return findAll(query, new EntityObjectRowMapper(query));
  }

  public Page<SearchRecord> findRecords(EntitySelectBuilder query) {
    return findAll(query, new SearchRecordRowMapper(query.getSearchFields()));
  }

  public List<String> findUniqueValues(EntityType entityType, String codeName) {
    Optional<Field> field = entityType.getField(codeName);
    if (field.isPresent()) {
      String sql = "SELECT DISTINCT " + field.get().getCodeName()
          + " FROM " + entityTypeTable(entityType);
      log.trace("{} objects select query:\n{}", entityType.getCodeName(), sql);
      return jdbcTemplate.queryForList(sql, String.class);
    } else {
      throw new FieldNotFoundException(codeName);
    }
  }

  /**
   * Загрузить объект только со стандартыми атрибутами
   *
   * @param entityType класс объектов
   * @param id идентификатор объекта
   * @return найденный объект
   */
  public Optional<EntityObject> findOneBase(EntityType entityType, int id) {
    return findOne(entityType, id, Collections.emptyList());
  }

  /**
   * Загрузить объект только со стандартыми атрибутами
   *
   * @param entityType класс объектов
   * @param guid глобальный идентификатор объекта
   * @return найденный объект
   */
  public Optional<EntityObject> findOneBase(EntityType entityType, UUID guid) {
    return findOne(entityType, guid, Collections.emptyList());
  }

  /**
   * Загрузить объект со всеми атрибутами
   *
   * @param entityType класс объектов
   * @param id идентификатор объекта
   * @return найденный объект
   */
  public Optional<EntityObject> findOne(EntityType entityType, int id) {
    return findOne(entityType, id, entityType.getFields());
  }

  /**
   * Загрузить объект со всеми атрибутами
   *
   * @param entityType класс объектов
   * @param guid глобальный идентификатор объекта
   * @return найденный объект
   */
  public Optional<EntityObject> findOne(EntityType entityType, UUID guid) {
    return findOne(entityType, guid, entityType.getFields());
  }

  /**
   * Загрузить объект со всеми атрибутами
   *
   * @param entityType класс объектов
   * @param name имя объекта
   * @return найденный объект
   */
  public Optional<EntityObject> findOneByName(EntityType entityType, String name) {
    return findOneByName(entityType, name, entityType.getFields());
  }

  /**
   * Загрузить объект с определенными атрибутами
   *
   * @param entityType класс объектов
   * @param name имя объекта
   * @param fields список атрибутов, которые нужно получить
   * @return найденный объект
   */
  public Optional<EntityObject> findOneByName(EntityType entityType, String name,
                                               List<BaseField> fields) {
    EntitySelectBuilder query = preFind(entityType, fields).withName(name);
    return findOne(query);
  }

  /**
   * Загрузить объект с определенными атрибутами
   *
   * @param entityType класс объектов
   * @param id идентификатор объекта
   * @param fields список атрибутов, которые нужно получить
   * @return найденный объект
   */
  public Optional<EntityObject> findOne(EntityType entityType, int id, List<BaseField> fields) {
    EntitySelectBuilder query = preFind(entityType, fields).withId(id);
    return findOne(query);
  }

  public Optional<EntityObject> findOne(EntitySelectBuilder query) {
    String sql = query.build();
    log.trace("{} objects select query:\n{}", query.getEntityType().getCodeName(), sql);
    return jdbcTemplate.query(sql, new EntityObjectRowMapper(query), query.getParams())
        .stream()
        .findFirst();
  }

  /**
   * Загрузить объект с определенными атрибутами и геометрией в заданной проекции
   *
   * @param entityType класс объектов
   * @param id идентификатор объекта
   * @param srid код проекции
   * @return найденный объект
   */
  public Optional<EntityObject> findOne(EntityType entityType, int id, int srid) {
    EntitySelectBuilder query = preFind(entityType, entityType.getFields()).withId(id).srid(srid);
    return findOne(query);
  }

  /**
   * Сохранить объект. Если {@link EntityObject#getId()} == 0 будет создан новый объект. Если id
   * задан - объект будет обновлен
   *
   * @param entityType класс объектов
   * @param object объект
   * @return сохраненный объект
   */
  public EntityObject save(@NonNull EntityType entityType, @NonNull EntityObject object) {
    if (object.isNew()) {
      insert(entityType, object);
    } else {
      update(entityType, object);
    }
    return object;
  }

  /**
   * Удалить объект
   *
   * @param entityType класс объектов
   * @param object объект
   */
  public void delete(@NonNull EntityType entityType, @NonNull EntityObject object) {
    String pattern = "delete from {0} where id = ?";
    String sql = format(pattern, entityTypeTable(entityType));
    log.trace("object {}#{} remove query:\n{}", entityType.getCodeName(), object.getId());
    jdbcTemplate.update(sql, object.getId());
  }

  public Point entityCentroid(GeometryField field, long objectId, int srid) {
    String tableName = DbNameProvider.entityTypeTable(field.getEntityType());
    String geomCol = field.getCodeName();
    return queryColumnCentroid(tableName, geomCol, objectId, srid);
  }

  public Extent entityExtent(GeometryField field, long objectId, int srid) {
    String tableName = DbNameProvider.entityTypeTable(field.getEntityType());
    String geomCol = field.getCodeName();
    return queryColumnExtent(tableName, geomCol, objectId, srid);
  }

  /**
   * Получить количество объектов класса
   *
   * @param entityType класс объектов
   * @return количество объектов класса
   */
  public int count(EntityType entityType) {
    return count(selectBuilderFactory.newBuilderUnsecured(entityType));
  }

  /**
   * Получить количество объектов класса
   *
   * @param query запрос
   * @return количество объект класса
   */
  public int count(EntitySelectBuilder query) {
    return jdbcTemplate.queryForList(query.count(), Integer.class, query.getParams())
        .stream().findFirst().orElse(0);
  }

  private Point queryColumnCentroid(String tableName, String geomCol, long objectId, int srid) {
    String sql = String
        .format(
            "select ST_AsText(ST_Centroid(ST_Transform(%s, ?))) as cnt from %s where id = ?",
            geomCol, tableName);
    return queryCentroid(sql, objectId, srid);
  }

  private Point queryCentroid(String sql, long objectId, int srid) {
    try (Connection connection = jdbcTemplate.getDataSource()
        .getConnection(); PreparedStatement pstmt = connection.prepareStatement(sql)) {
      pstmt.setInt(1, srid);
      pstmt.setLong(2, objectId);
      try (ResultSet rs = pstmt.executeQuery()) {
        if (rs.next()) {
          return parseCentroid(rs.getString("cnt"));
        }
        return null;
      }
    } catch (SQLException e) {
      throw new EntityGeometryException("Centroid query sql error", e);
    }
  }

  private Point parseCentroid(String raw) {
    String[] coord = StringUtils.substringBetween(raw, "(", ")").split(" ");
    double x = Double.parseDouble(coord[0]);
    double y = Double.parseDouble(coord[1]);
    return new Point(x, y);
  }

  private Extent queryColumnExtent(String tableName, String geomCol, long objectId, int srid) {
    String sql = String.format("select ST_Extent(ST_Transform(%s, ?)) as ext from %s where id = ?",
                               geomCol, tableName);
    return queryExtent(sql, objectId, srid);
  }

  private Extent queryExtent(String sql, long objectId, int srid)
      throws EntityGeometryException {
    try (Connection connection = jdbcTemplate.getDataSource()
        .getConnection(); PreparedStatement pstmt = connection.prepareStatement(sql)) {
      pstmt.setInt(1, srid);
      pstmt.setLong(2, objectId);
      try (ResultSet rs = pstmt.executeQuery()) {
        if (rs.next()) {
          return parseExtent(rs.getString("ext"), srid);
        }
        return null;
      }
    } catch (SQLException e) {
      throw new EntityGeometryException("Extent query sql error", e);
    }
  }

  private Extent parseExtent(String rawExtent, int srid) {
    if (StringUtils.isBlank(rawExtent)) {
      return null;
    }
    if (!rawExtent
        .matches("BOX\\(-?\\d+(\\.\\d+)? -?\\d+(\\.\\d+)?,-?\\d+(\\.\\d+)? -?\\d+(\\.\\d+)?\\)")) {
      throw new EntityGeometryException("Extent doesn't matches pattern: " + rawExtent);
    }
    Extent result = new Extent();
    String[] coordinates = rawExtent.substring(4, rawExtent.length() - 1).split(",");
    String min[] = coordinates[0].split(" ");
    String max[] = coordinates[1].split(" ");
    result.setMinX(Double.parseDouble(min[0]));
    result.setMinY(Double.parseDouble(min[1]));
    result.setMaxX(Double.parseDouble(max[0]));
    result.setMaxY(Double.parseDouble(max[1]));
    return result;
  }

  private Optional<EntityObject> findOne(EntityType entityType, UUID guid, List<BaseField> fields) {
    EntitySelectBuilder query = preFind(entityType, fields).withGuid(guid);
    return findOne(query);
  }

  private EntitySelectBuilder preFind(EntityType entityType, List<BaseField> fields) {
    return selectBuilderFactory.newBuilder(entityType)
        .withFields(standardFields(entityType))
        .withFields(fields.toArray(new Field[0]));
  }

  private <T> Page<T> findAll(EntitySelectBuilder query, RowMapper<T> rowMapper) {
    List<T> objects = findList(query, rowMapper);
    if (query.getPageable() != null) {
      int size = count(query);
      return new PageImpl<>(objects, query.getPageable(), size);
    } else {
      return new PageImpl<>(objects);
    }
  }

  private <T> List<T> findList(EntitySelectBuilder query, RowMapper<T> rowMapper) {
    String sql = query.build();
    log.trace("objects select query:\n{}", sql);
    return jdbcTemplate.query(sql, rowMapper, query.getParams());
  }

  private void insert(EntityType entityType, EntityObject object) {
    Map<Field, Object> valueMap = fullAttributeValueMap(entityType, object);
    valueMap.remove(ID);
    object.setEntityType(entityType.getCodeName());

    int id = insertInnerFields(object, valueMap);
    object.setId(id);

    valueMap.entrySet().stream()
        .filter(e -> innerField.negate().test(e.getKey()))
        .forEach(e -> insertRelation(object.getId(), (RelationField) e.getKey(), e.getValue()));
    log.debug("created object {}#{}", entityType.getCodeName(), object.getId());
  }

  private int insertInnerFields(EntityObject object, Map<Field, Object> valueMap) {
    GeneratedKeyHolder keyHolder = new GeneratedKeyHolder();
    jdbcTemplate.update(con -> {
      String format = "insert into {0} ({1}) VALUES ({2})";
      StringJoiner columns = new StringJoiner(",");
      StringJoiner parameters = new StringJoiner(",");
      List<Object> values = new ArrayList<>();
      valueMap.entrySet().stream()
          .filter(e -> innerField.test(e.getKey()))
          .filter(e -> e.getValue() != null)
          .forEach(entry -> {
            Field field = entry.getKey();
            columns.add(field.getCodeName());
            parameters.add(prepareParam(field, entry.getValue()));
            values.add(prepareParamValue(con, field, entry.getValue()));
          });

      String tableName = entityTypeTable(object.getEntityType());
      String query = format(format, tableName, columns, parameters);
      log.trace("{} insert query is:\n{}", object.getEntityType(), query);

      PreparedStatement statement = con.prepareStatement(query, new String[]{ID.getCodeName()});
      for (int i = 0; i < values.size(); i++) {
        statement.setObject(i + 1, values.get(i));
      }
      return statement;
    }, keyHolder);

    return keyHolder.getKey().intValue();
  }

  private void update(EntityType entityType, EntityObject object) {
    object.setEntityType(entityType.getCodeName());
    Map<Field, Object> valueMap = fullAttributeValueMap(entityType, object);
    valueMap.remove(ID);
    valueMap.remove(CREATE_DATE);
    valueMap.remove(CREATE_USER);
    valueMap.remove(GUID);

    updateInnerFields(object, valueMap);
    valueMap.entrySet().stream()
        .filter(e -> innerField.negate().test(e.getKey()))
        .forEach(e -> updateRelation(object.getId(), (RelationField) e.getKey(), e.getValue()));
  }

  private void updateRelation(int parentId, RelationField field, Object value) {
    List<Integer> related = convertToIdList(field, value);
    if (hasRelationTable.test(field)) {
      clearWithRelationTable(parentId, field);
      insertWithRelationTable(parentId, field, related);
    } else {
      clearReverseField(parentId, field);
      insertReverseField(parentId, field, related);
    }
  }

  private void insertRelation(int parentId, RelationField field, Object value) {
    List<Integer> related = convertToIdList(field, value);
    if (hasRelationTable.test(field)) {
      insertWithRelationTable(parentId, field, related);
    } else {
      insertReverseField(parentId, field, related);
    }
  }

  private void updateInnerFields(EntityObject object, Map<Field, Object> valueMap) {
    jdbcTemplate.update(con -> {
      String format = "update {0} set {1} where id = ?";
      List<Object> params = new ArrayList<>();
      StringJoiner columns = new StringJoiner(",");

      valueMap.entrySet().stream().filter(entry -> innerField.test(entry.getKey()))
          .forEach(entry -> {
            Field field = entry.getKey();
            params.add(prepareParamValue(con, field, entry.getValue()));
            columns.add(field.getCodeName() + "=" + prepareParam(field, entry.getValue()));
          });

      String tableName = entityTypeTable(object.getEntityType());
      String query = format(format, tableName, columns);
      log.trace("{}#{} update query is\n{}", object.getEntityType(), object.getId(), query);
      params.add(object.getId());

      PreparedStatement statement = con
          .prepareStatement(query, new String[]{ID.getCodeName()});
      for (int i = 0; i < params.size(); i++) {
        statement.setObject(i + 1, params.get(i));
      }
      return statement;
    });
  }

  private void clearReverseField(int id, RelationField field) {
    String sql = format("update {0} set {1} = null where {1} = ?",
                        entityTypeTable(field.getRelates()), field.getReverseFieldCode());
    log.trace("{} attribute clear query is:\n{}", field.getCodeName(), sql);
    jdbcTemplate.update(sql, id);
  }

  private void insertReverseField(int id, RelationField field, List<Integer> related) {
    String sql = format("update {0} set {1} = ? where id = ?",
                        entityTypeTable(field.getRelates()), field.getReverseFieldCode());
    log.trace("{} attribute update query:\n", field.getCodeName(), sql);
    jdbcTemplate.batchUpdate(sql, new RelationBatchUpdate(id, related));
  }

  private void insertWithRelationTable(int id, RelationField field, List<Integer> related) {
    String src = fieldSrcColumn(field);
    String dst = fieldDstColumn(field);

    String sql = format("insert into {0} ({1}, {2}) values (?, ?)",
                        relationTable(field), src, dst);
    log.trace("{} attribute insert query:\n", field.getCodeName(), sql);
    jdbcTemplate.batchUpdate(sql, new RelationBatchUpdate(id, related));
  }

  private void clearWithRelationTable(int id, RelationField field) {
    String sql = format("delete from {0} where {1} = ?", relationTable(field),
                        fieldSrcColumn(field));
    log.trace("{} attribute delete query:\n{}", field.getCodeName(), sql);
    jdbcTemplate.update(sql, id);
  }

  private Object prepareParamValue(Connection connection, Field field, Object value) {
    if (value == null) {
      return null;
    }
    FieldType fieldType = field.getFieldType();
    if (field.isMultiple()) {
      return prepareMultipleValues(connection, fieldType, (Object[]) value);
    } else {
      return convertSingleValue(fieldType, value);
    }
  }

  private Array prepareMultipleValues(Connection connection, FieldType fieldType, Object[] values) {
    Object[] converted = Arrays.stream(values)
        .map(v -> convertSingleValue(fieldType, v))
        .toArray();
    return SqlArrayUtils.createArrayOf(connection, fieldType, converted);
  }

  private Object convertSingleValue(FieldType fieldType, Object value) {
    if (fieldType == FieldType.DATE) {
      return Date.valueOf((LocalDate) value);
    } else if (fieldType == FieldType.DATE_TIME) {
      return Timestamp.valueOf((LocalDateTime) value);
    } else if (fieldType == FieldType.TIME) {
      return Time.valueOf((LocalTime) value);
    } else if (fieldType == FieldType.RELATION) {
      return ((EntityObject) value).getId();
    } else if (fieldType == FieldType.GEOMETRY) {
      return ((Geometry) value).getGeometry();
    } else if (fieldType == FieldType.ATTACHMENT) {
      return mapToJsonString(value);
    } else {
      return value;
    }
  }

  private List<Integer> convertToIdList(RelationField field, Object value) {
    List<Integer> values = new ArrayList<>();
    if (value == null) {
      return values;
    }
    if (field.isMultiple()) {
      Arrays.stream((Object[]) value)
          .map(o -> (EntityObject) o)
          .map(EntityObject::getId)
          .forEach(values::add);
    } else {
      values.add(((EntityObject) value).getId());
    }
    return values;
  }

  private String prepareParam(Field field, Object value) {
    if (field.getFieldType() == FieldType.GEOMETRY && value != null) {
      Geometry geomValue = (Geometry) value;
      return format(GEOM_FUNCTION, geomValue.getType() == Type.WKT ? ST_FROM_TEXT : ST_FROM_GEOJSON,
                    ((GeometryField) field).getCrs());
    } else if (field.getFieldType() == FieldType.ATTACHMENT) {
      return "?::JSONB";
    } else {
      return "?";
    }
  }
}
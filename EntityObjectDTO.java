package com.example.core.objects.api.dto;

import com.example.core.common.rest.MetadataDTO;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class EntityObjectDTO {

  private int id;

  private String name;

  private String guid;

  private String entityType;

  private Integer parentId;

  private MetadataDTO metadata;

  private int status;

  private Map<String, Object> attributes;

  private List<ObjectAttachment> attachments;

}

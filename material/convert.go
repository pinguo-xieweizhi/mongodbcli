package material

import (
	"context"
	"sort"

	"github.com/pinguo-icc/field-definitions/api"
	"github.com/pinguo-icc/field-definitions/pkg/fieldvalue"
	mapi "github.com/pinguo-icc/operational-materials-svc/api"
)

func apiFieldsDefinition(d *FieldsDefinition) *api.FieldsDefinition {
	if d == nil {
		return nil
	}
	return &api.FieldsDefinition{
		Id:     d.ID,
		Name:   d.Name,
		Scope:  d.Scope,
		Type:   api.FieldCategory(d.Type),
		Fields: apiFields(d.Fields),
		Config: &api.FieldsDefinition_Configs{
			SystemFieldNameIsUnique: d.Configs.SystemFieldNameIsUnique,
			CoverCode:               d.Configs.CoverCode,
			ChildRefCode:            d.Configs.ChildRefCode,
			PackageCodes:            d.Configs.PackageCodes,
		},
	}
}

func apiFields(d []*Field) []*api.Field {
	r := make([]*api.Field, len(d))
	for i := range d {
		r[i] = apiField(d[i])
	}
	return r
}

func apiField(d *Field) *api.Field {
	if d == nil {
		return nil
	}

	field := &api.Field{
		Code:                d.Code,
		Name:                d.Name,
		Comment:             d.Comment,
		Required:            d.Required,
		IsSystematized:      d.IsSystematized,
		IsLocalized:         d.IsLocalized,
		SysFieldType:        api.SystemFieldType(d.SysFieldType),
		CustomizedFieldType: api.CustomizedFieldType(d.CustomizedFieldType),
	}

	if d.CustomizedFieldType != 0 {
		switch d.CustomizedFieldType {
		case CustomizedFieldText:
			field.Data = &api.Field_Text{
				Text: &api.CustomizedTextConfig{
					Type: api.CustomizedTextConfig_Type(d.TextField.Type),
					Hint: d.TextField.Hint,
				},
			}
		case CustomizedFieldNumberRange:
			field.Data = &api.Field_NumberRange{
				NumberRange: &api.CustomizedNumberRangeConfig{},
			}
		case CustomizedFieldDatetime:
			field.Data = &api.Field_Datetime{
				Datetime: &api.CustomizedDatetimeConfig{
					Type: api.CustomizedDatetimeConfig_Type(d.DatetimeField.Type),
				},
			}
		case CustomizedFieldEnumeration:
			data := &api.Field_Enumeration{
				Enumeration: &api.CustomizedEnumerationConfig{
					IsMultiple: d.EnumField.IsMultiple,
					ValueType:  int32(d.EnumField.ValueType),
					Items:      make([]*api.EnumItem, len(d.EnumField.EnumItems)),
				},
			}
			for i, v := range d.EnumField.EnumItems {
				data.Enumeration.Items[i] = &api.EnumItem{
					Label: v.Label,
					Value: v.Value,
				}
			}
			field.Data = data
		case CustomizedFieldFile:
			field.Data = &api.Field_File{
				File: &api.CustomizedFileConfig{},
			}
		case CustomizedFieldImage:
			data := &api.Field_Image{
				Image: &api.CustomizedImageConfig{
					SupportedSize: make([]*api.ImageSize, len(d.ImageField.SupportedSize)),
				},
			}
			for i, v := range d.ImageField.SupportedSize {
				data.Image.SupportedSize[i] = &api.ImageSize{
					Width:  int32(v.Width),
					Height: int32(v.Height),
				}
			}
			field.Data = data
		case CustomizedFieldMaterialReference:
			field.Data = &api.Field_MaterialReference{
				MaterialReference: &api.CustomizedMaterialReferenceConfig{
					Id:     d.MaterialReferenceField.ID,
					IsPack: d.MaterialReferenceField.IsPack,
				},
			}
		case CustomizedVideo:
			field.Data = &api.Field_Video{
				Video: &api.CustomizedVideoConfig{},
			}
		case CustomizedGoto:
			fd := &api.Field_Goto{
				Goto: &api.CustomizedGotoConfig{
					EnabledTypes:  make([]api.CustomizedGotoConfig_Type, 0),
					MaterialCodes: []string{},
				},
			}
			if d.GotoField != nil {
				fd.Goto.MaterialCodes = d.GotoField.MaterialCodes
				fd.Goto.EnabledTypes = make([]api.CustomizedGotoConfig_Type, len(d.GotoField.EnabledTypes))
				for idx, v := range d.GotoField.EnabledTypes {
					fd.Goto.EnabledTypes[idx] = api.ParseGoToType(v)
				}
			}
			field.Data = fd
		case CustomizedTag:
			fd := &api.Field_Tag{
				Tag: &api.CustomizedTagConfig{
					Type: "",
				},
			}
			if d.TagField != nil {
				fd.Tag.Type = d.TagField.TagType
			}
			field.Data = fd
		case CustomizedNumber:
			fd := &api.Field_Number{
				Number: &api.CustomizedNumberConfig{
					Type: api.CustomizedNumberConfig_Type(d.NumberField.Type),
					Hint: d.NumberField.Hint,
				},
			}

			field.Data = fd
		case CustomizedColor:
			fd := &api.Field_Color{
				Color: &api.CustomizedColorConfig{},
			}

			field.Data = fd

		}
	}

	return field
}

func convertMaterialToAPI(
	ctx context.Context,
	md *Material,
) *mapi.Material {
	tmp := &mapi.Material{
		Id:    md.ID.Hex(),
		Type:  md.TypeID,
		Scope: md.Scope,
	}

	// TODO: 需支持跳过一些字段不下发 skipFieldLabel ？？
	versions := []*mapi.MaterialVersion{}
	for i := range md.Versions {
		v := md.Versions[i]
		cv := &mapi.MaterialVersion{
			VersionID:     v.VersionID.Hex(),
			VersionName:   v.VersionName,
			Name:          v.Name,
			Vip:           int32(v.Vip),
			Tag:           v.Tag,
			Status:        string(v.Status),
			UpdatedAt:     v.UpdatedAt.Unix(),
			CreatedAt:     v.CreatedAt.Unix(),
			Creator:       v.Creator,
			Mender:        v.Mender,
			ValidDuration: &v.ValidDuration,
			ClientVersion: &v.ClientVersion,
			Custom:        v.Custom,
			Localize:      sortLocalAttributesByPropties(v.Localize),
		}

		for _, p := range v.Platform {
			cv.Platform = append(cv.Platform, string(p))
		}

		versions = append(versions, cv)
	}

	tmp.Versions = versions

	return tmp
}

func convertCategoryToAPI(
	ctx context.Context, catData *Category, skipFieldLabel bool,
) *mapi.Category {
	d := &mapi.Category{
		Id:        catData.ID.Hex(),
		Scope:     catData.Scope,
		SortOrder: int32(catData.SortOrder),
		Parent:    catData.Parent,
		Type:      catData.TypeID,
	}

	for i := range catData.Versions {
		cv := catData.Versions[i]
		d.Versions = append(d.Versions, &mapi.CategoryVersion{
			VersionID:   cv.VersionID.Hex(),
			VersionName: cv.VersionName,
			Name:        cv.Name,
			CreatedAt:   cv.CreatedAt.Unix(),
			UpdatedAt:   cv.CreatedAt.Unix(),
			Creator:     cv.Creator,
			Mender:      cv.Mender,
			Custom:      cv.Custom,
			Localize:    sortLocalAttributesByPropties(cv.Localize),
		})
	}

	return d
}

func splitByLocalType(lattr []*fieldvalue.Localize) (def, other []*fieldvalue.Localize) {
	for i := range lattr {
		la := lattr[i]
		if la.ClientLocale.Typ == "default" {
			def = append(def, la)
		} else {
			other = append(other, la)
		}
	}

	return def, other
}

// sortLocalAttributesByPropties 根据本地化字段优先级属性进行排序
func sortLocalAttributesByPropties(local []*fieldvalue.Localize) []*fieldvalue.Localize {
	if len(local) <= 1 {
		return local
	}

	type sortItem struct {
		priority float64
		attr     *fieldvalue.Localize
	}

	sortItems := make([]sortItem, 0, len(local))
	def, other := splitByLocalType(local)

	if len(def) > 0 {
		for i := range def {
			tmp := def[i]
			sortItems = append(sortItems, sortItem{
				priority: -65536,
				attr:     tmp,
			})
		}
	}

	for i := range other {
		tmp := other[i]
		sortItems = append(sortItems, sortItem{
			priority: float64(tmp.Priority),
			attr:     tmp,
		})
	}

	sort.Slice(sortItems, func(i, j int) bool {
		return sortItems[i].priority < sortItems[j].priority
	})

	sorted := make([]*fieldvalue.Localize, 0, len(sortItems))
	for _, v := range sortItems {
		sorted = append(sorted, v.attr)
	}

	return sorted
}

func materialPositionToAPI(e *MaterialPosition) *mapi.MaterialPosition {
	if e == nil {
		return nil
	}

	res := &mapi.MaterialPosition{
		Id:                         e.ID.Hex(),
		Scope:                      e.Scope,
		Code:                       e.Code,
		Name:                       e.Name,
		MaterialTypeId:             e.MaterialTypeID,
		ContainsCategory:           e.ContainsCategory,
		Platform:                   e.Platform,
		ClientVersion:              make(map[string]*mapi.ClientVersion),
		DeviceModelLimited:         e.DeviceLimited[DeviceLimitedModel],
		DeviceSystemVersionLimited: e.DeviceLimited[DeviceLimitedSystemVersion],
		Summary:                    e.Summary,
		Active:                     e.Active,
		GotoId:                     e.GotoId,
		CreatedAt:                  e.CreatedAt.Unix(),
		UpdatedAt:                  e.UpdatedAt.Unix(),
		Creator:                    e.Creator,
		Editor:                     e.Editor,
	}

	for p, v := range e.ClientVersion {
		res.ClientVersion[p] = &mapi.ClientVersion{
			Min:         v.Min.String(),
			Max:         v.Max.String(),
			Unsupported: make([]string, len(v.Unsupported)),
		}
		for i := range v.Unsupported {
			res.ClientVersion[p].Unsupported[i] = v.Unsupported[i].String()
		}
	}

	return res
}

var cplan = plan{}

type plan struct{}

func (p plan) APIPlan(d *Plan) *mapi.Plan {
	return &mapi.Plan{
		Id:            d.ID.Hex(),
		PosId:         d.PosID.Hex(),
		Scope:         d.Scope,
		Type:          string(d.Type),
		Name:          d.Name,
		Active:        d.Active,
		Priority:      uint32(d.Priority),
		Period:        APIPeriod(&d.Period),
		UserSelection: p.APIUserSelection(&d.UserSelection),
		Creator:       d.Creator,
		Editor:        d.Editor,
		CreatedAt:     d.CreatedAt.Unix(),
		UpdatedAt:     d.UpdatedAt.Unix(),
		Contents:      SliceTo(d.PlacingContent, p.APIPlacingContent),
	}
}

func APIPeriod(p *Period) *mapi.Period {
	if p == nil {
		return nil
	}

	r := &mapi.Period{
		Begin: p.Begin.Unix(),
	}
	if !p.End.IsZero() {
		r.End = p.End.Unix()
	}

	return r
}

func (p plan) APIUserSelection(d *UserSelection) *mapi.UserSelection {
	if d == nil {
		return nil
	}

	r := &mapi.UserSelection{Type: mapi.UserSelection_Type(d.Type)}
	switch d.Type {
	case UserGroups:
		r.UserGroupsId = d.UserGroupsID
	case UserProperties:
		r.UserProperties = SliceTo(d.UserProperties, p.APIUserSelectionProperty)
	}

	return r
}

func SliceTo[A, B any](a []A, fn func(A) B) []B {
	r := make([]B, len(a))
	for i := range a {
		r[i] = fn(a[i])
	}
	return r
}

func (plan) APIUserSelectionProperty(d *UserSelectionProperty) *mapi.UserSelection_UserProperty {
	if d == nil {
		return nil
	}

	return &mapi.UserSelection_UserProperty{
		Code:  d.Code,
		Op:    d.Op,
		Value: d.Value,
	}
}

func (p plan) APIPlacingContent(d *PlacingContent) *mapi.PlacingContent {
	r := &mapi.PlacingContent{
		Id:    d.ID.Hex(),
		Ratio: uint32(d.Ratio),
	}
	if d.CategoryTypeDefID != "" {
		r.Data = &mapi.PlacingContent_CategoryValue{
			CategoryValue: &mapi.PlacingContent_CategoryContent{
				CategoryTypeDefId: d.CategoryTypeDefID,
				Categories:        SliceTo(d.Categories, p.APIPlacingCategory),
			},
		}
	} else {
		r.Data = &mapi.PlacingContent_MaterialValue{
			MaterialValue: &mapi.PlacingContent_MaterialList{
				List: SliceTo(d.Materials, p.APIPlacingMaterial),
			},
		}
	}

	return r
}

func (p plan) APIPlacingCategory(d *PlacingCategory) *mapi.PlacingContent_Category {
	return &mapi.PlacingContent_Category{
		Id:        d.ID,
		Pid:       d.PID,
		Active:    d.Active,
		Period:    APIPeriod(d.Period),
		Materials: SliceTo(d.Materials, p.APIPlacingMaterial),
	}
}

func (plan) APIPlacingMaterial(d *PlacingMaterial) *mapi.PlacingContent_Material {
	return &mapi.PlacingContent_Material{
		Id:        d.ID,
		Vip:       d.VIP,
		Period:    APIPeriod(d.Period),
		VersionID: d.VersionID,
	}
}

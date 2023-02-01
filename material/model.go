package material

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/pinguo-icc/field-definitions/api"
	"github.com/pinguo-icc/field-definitions/pkg"
	"github.com/pinguo-icc/field-definitions/pkg/fieldvalue"
	"github.com/pinguo-icc/go-base/v2/version"
	"github.com/pinguo-icc/go-lib/v2/dao"
	ldao "github.com/pinguo-icc/go-lib/v2/dao"
	"github.com/pinguo-icc/kratos-library/mongo/op"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type DatePeriod struct {
	StartDate int64 `bson:"startDate"`
	EndDate   int64 `bson:"endDate"`
}

type CustomAttribute struct {
	Code      string      `bson:"code"`
	FieldType string      `bson:"fieldType"`
	Value     *FieldValue `bson:"value"`
}

func (ca *CustomAttribute) convert(field *api.Field) *fieldvalue.FieldValue {
	if ca == nil || ca.Value == nil {
		return nil
	}

	return ca.Value.convert(field)
}

// NumberRangeValue 数值范围
type NumberRangeValue struct {
	Value []float64 `bson:"value"`
}

func (nrv *NumberRangeValue) convert() []float64 {
	if nrv == nil {
		return []float64{}
	}

	return nrv.Value
}

// TextValue 文本值
type TextValue struct {
	Value string `bson:"value"`
}

func (tv *TextValue) convert() *string {
	if tv == nil {
		return nil
	}

	return &tv.Value
}

// NumberValue 数字值
type NumberValue struct {
	Value float64 `bson:"value"`
}

func (nv *NumberValue) convert() *float64 {
	if nv == nil {
		return nil
	}

	return &nv.Value
}

func (nv *NumberValue) ConvertInt() *int64 {
	if nv == nil {
		return nil
	}
	res := int64(nv.Value)

	return &res
}

// DateTimeValue 日期时间, 为2元素的数组
type DateTimeValue struct {
	Value []int64 `bson:"value"`
}

func (dtv *DateTimeValue) ConvertToTimePoint() *int64 {
	if dtv == nil || len(dtv.Value) == 0 {
		return nil
	}

	return &dtv.Value[0]
}

func (dtv *DateTimeValue) ConvertToTimeInterval() []int64 {
	if dtv == nil {
		return []int64{}
	}

	return dtv.Value
}

// EnumValue 枚举值
type EnumValue struct {
	Value []string `bson:"value"`
}

func (ev *EnumValue) ConvertToSingleString() *string {
	if ev == nil || len(ev.Value) == 0 {
		return nil
	}

	return &ev.Value[0]
}

func (ev *EnumValue) ConvertToMutilString() []string {
	if ev == nil {
		return []string{}
	}

	return ev.Value
}

func (ev *EnumValue) ConvertToSingleInt() *int64 {
	if ev == nil || len(ev.Value) == 0 {
		return nil
	}

	v := ev.Value[0]
	res, _ := strconv.ParseInt(v, 10, 64)

	return &res
}

func (ev *EnumValue) ConvertToMutilInt() []int64 {
	if ev == nil {
		return []int64{}
	}

	res := make([]int64, 0, len(ev.Value))
	for _, v := range ev.Value {
		cv, _ := strconv.ParseInt(v, 10, 64)
		res = append(res, cv)
	}

	return res
}

func (ev *EnumValue) ConvertToSingleFloat() *float64 {
	if ev == nil || len(ev.Value) == 0 {
		return nil
	}

	v := ev.Value[0]
	res, _ := strconv.ParseFloat(v, 10)

	return &res
}

func (ev *EnumValue) ConvertToMutilFloat() []float64 {
	if ev == nil {
		return []float64{}
	}

	res := make([]float64, 0, len(ev.Value))
	for _, v := range ev.Value {
		cv, _ := strconv.ParseFloat(v, 10)
		res = append(res, cv)
	}

	return res
}

func (ev *EnumValue) ConvertToBool() *bool {
	if ev == nil || len(ev.Value) == 0 {
		return nil
	}

	v := ev.Value[0]
	res, _ := strconv.ParseBool(v)

	return &res
}

// FileValue 文件
type FileValue struct {
	URI       string `bson:"uri"`
	Size      int64  `bson:"size"`
	Suffix    string `bson:"suffix"`
	IsPrivate bool   `bson:"isPrivate"`
}

func (fv *FileValue) convert() *fieldvalue.File {
	if fv == nil {
		return nil
	}

	return &fieldvalue.File{
		Uri:       fv.URI,
		Url:       "",
		Ext:       fv.Suffix,
		Size:      fv.Size,
		Expire:    0,
		IsPrivate: fv.IsPrivate,
		Path:      "",
	}
}

// VideoValue 视频
type VideoValue struct {
	FileValue  `bson:",inline" json:",inline"`
	Width      int64       `bson:"width"`
	Height     int64       `bson:"height"`
	Screenshot *ImageValue `bson:"screenshot"` // 视频截图
}

func (vv *VideoValue) convert() *fieldvalue.Video {
	if vv == nil {
		return nil
	}

	res := &fieldvalue.Video{
		Uri:        vv.URI,
		Url:        "",
		Ext:        vv.Suffix,
		Size:       vv.Size,
		Expire:     0,
		IsPrivate:  vv.IsPrivate,
		Width:      int32(vv.Width),
		Height:     int32(vv.Height),
		Path:       "",
		Screenshot: vv.Screenshot.convert(),
	}

	return res
}

// ImageValue 图片
type ImageValue struct {
	FileValue `bson:",inline" json:",inline"`
	Width     int64 `bson:"width"`
	Height    int64 `bson:"height"`
}

func (iv *ImageValue) convert() *fieldvalue.Image {
	if iv == nil {
		return nil
	}

	return &fieldvalue.Image{
		Uri:       iv.URI,
		Url:       "",
		Ext:       iv.Suffix,
		Size:      iv.Size,
		Expire:    0,
		IsPrivate: iv.IsPrivate,
		Width:     int32(iv.Width),
		Height:    int32(iv.Height),
		Path:      "",
	}
}

// ReferenceValue 引用
type ReferenceValue struct {
	Value    []string              `bson:"value"`
	PkgUrl   string                `bson:"pkgUrl"`
	PackInfo map[string]*FileValue `bson:"packInfo"` // {"defaultLocal":FileValue,"zh-Hans":FileValue}
}

func (rv *ReferenceValue) convert() *fieldvalue.Reference {
	if rv == nil {
		return nil
	}

	res := &fieldvalue.Reference{
		Value:  rv.Value,
		PkgUrl: rv.PkgUrl,
		Type:   fieldvalue.ReferenceType_Material,
	}

	if rv.PackInfo == nil {
		return res
	}

	respi := make(map[string]*fieldvalue.File, len(rv.PackInfo))
	for k := range rv.PackInfo {
		v := rv.PackInfo[k]
		respi[k] = v.convert()
	}

	res.PackInfo = respi

	return res
}

type ClientVersion struct {
	Min         version.SemVer   `json:"min" bson:"min"`
	Max         version.SemVer   `json:"max" bson:"max"`
	Unsupported []version.SemVer `json:"unsupported" bson:"unsupported"`
}

func (cv *ClientVersion) convert() *fieldvalue.ClientVersion {
	if cv == nil {
		return nil
	}

	res := &fieldvalue.ClientVersion{
		Min: cv.Min.String(),
		Max: cv.Max.String(),
	}

	for _, v := range cv.Unsupported {
		res.Unsupported = append(res.Unsupported, v.String())
	}

	return res
}

// VersionValue [platform]*ClientCompatibility
type VersionValue map[string]*ClientVersion

func (vv VersionValue) convert() fieldvalue.Version {
	res := fieldvalue.Version{}
	if vv == nil {
		return res
	}

	nvv := make(map[string]*fieldvalue.ClientVersion)
	for k := range vv {
		v := vv[k]
		nvv[k] = v.convert()
	}

	res.V = nvv

	return res
}

type LocalizeAttribute struct {
	SystemAttributes []*CustomAttribute `bson:"systemAttributes"`
	CustomAttributes []*CustomAttribute `bson:"customAttributes"`
}

func (la *LocalizeAttribute) convert(fd *api.FieldsDefinition) *fieldvalue.Localize {
	if la == nil {
		return nil
	}

	res := &fieldvalue.Localize{}

	for i := range la.SystemAttributes {
		v := la.SystemAttributes[i]
		if v == nil {
			continue
		}

		if v.Code == pkg.Code_Priority {
			if v.Value != nil && v.Value.NumberValue != nil {
				res.Priority = int32(v.Value.NumberValue.Value)
			}
		}

		if v.Code == pkg.Code_ClientLocale {
			if v.Value == nil {
				res.ClientLocale = nil
			}

			res.ClientLocale = v.Value.NameAndValuesValue.convert()
		}
	}

	res.Custom = convertCustomAttribuite(fd, la.CustomAttributes)

	return res
}

// NameAndValuesValue Key=>Values 键值
type NameAndValuesValue struct {
	Name  string   `bson:"name"`
	Value []string `bson:"value"`
}

func (navv *NameAndValuesValue) convert() *fieldvalue.ClientLocale {
	if navv == nil {
		return nil
	}

	return &fieldvalue.ClientLocale{
		Typ:      navv.Name,
		Selected: navv.Value,
	}
}

type NameAndValuesValues []*NameAndValuesValue

func (navvs NameAndValuesValues) convert() *fieldvalue.ClientLocale {
	if len(navvs) == 0 {
		return nil
	}

	return navvs[0].convert()
}

type TagValue struct {
	Value []string `bson:"value"`
}

func (tv *TagValue) convert() []string {
	if tv == nil {
		return []string{}
	}

	return tv.Value
}

type NumberIntValue struct {
	Value *int64 `bson:"value"`
}

func (niv *NumberIntValue) convert() *int64 {
	if niv == nil {
		return nil
	}

	return niv.Value
}

type NumberFloatValue struct {
	Value *float64 `bson:"value"`
}

func (nfv *NumberFloatValue) convert() *float64 {
	if nfv == nil {
		return nil
	}

	return nfv.Value
}

type ColorValue struct {
	Value string `bson:"value"`
}

func (cv *ColorValue) convert() *string {
	if cv == nil {
		return nil
	}

	return &cv.Value
}

// FieldValue 字段值
type FieldValue struct {
	TextValue          *TextValue          `bson:"textValue,omitempty" json:"txt,omitempty"`
	NumberValue        *NumberValue        `bson:"numberValue,omitempty" json:"num,omitempty"`
	NumberRangeValue   *NumberRangeValue   `bson:"numberRangeValue,omitempty" json:"numRange,omitempty"`
	DateTimeValue      *DateTimeValue      `bson:"dateTimeValue,omitempty" json:"date,omitempty"`
	EnumValue          *EnumValue          `bson:"enumValue,omitempty" json:"enum,omitempty"`
	FileValue          *FileValue          `bson:"fileValue,omitempty" json:"file,omitempty"`
	ImageValue         *ImageValue         `bson:"imageValue,omitempty" json:"image,omitempty"`
	VideoValue         *VideoValue         `bson:"videoValue,omitempty" json:"video,omitempty"`
	ReferenceValue     *ReferenceValue     `bson:"referenceValue,omitempty" json:"ref,omitempty"`
	NameAndValuesValue NameAndValuesValues `bson:"nameAndValuesValue,omitempty" json:"kv,omitempty"`
	VersionValue       *VersionValue       `bson:"versionValue,omitempty" json:"ver,omitempty"`
	TagValue           *TagValue           `bson:"tagValue,omitempty" json:"tag,omitempty"`
	NumberIntValue     *NumberIntValue     `bson:"numberInt,omitempty" json:"int,omitempty"`
	NumberFloatValue   *NumberFloatValue   `bson:"numberFloat,omitempty" json:"float,omitempty"`
	ColorValue         *ColorValue         `bson:"colorValue,omitempty" json:"color,omitempty"`
}

func (fv *FieldValue) convert(fd *api.Field) *fieldvalue.FieldValue {
	if fv == nil || fd == nil {
		return nil
	}

	switch fd.CustomizedFieldType {
	case api.CustomizedFieldType_Text:
		return &fieldvalue.FieldValue{
			Text: fv.TextValue.convert(),
		}
	case api.CustomizedFieldType_Number:
		if fd.GetNumber().Type == api.CustomizedNumberConfig_Int {
			return &fieldvalue.FieldValue{
				NumberInt: fv.NumberIntValue.convert(),
			}
		}
		if fd.GetNumber().Type == api.CustomizedNumberConfig_Float {
			return &fieldvalue.FieldValue{
				NumberFloat: fv.NumberFloatValue.convert(),
			}
		}

		return nil
	case api.CustomizedFieldType_NumberRange:
		return &fieldvalue.FieldValue{
			NumberFloatArray: fv.NumberRangeValue.convert(),
		}
	case api.CustomizedFieldType_Datetime:
		if fd.GetDatetime().Type == api.CustomizedDatetimeConfig_Point {
			return &fieldvalue.FieldValue{
				NumberInt: fv.DateTimeValue.ConvertToTimePoint(),
			}
		}
		if fd.GetDatetime().Type == api.CustomizedDatetimeConfig_Range {
			return &fieldvalue.FieldValue{
				NumberIntArray: fv.DateTimeValue.ConvertToTimeInterval(),
			}
		}

		return nil
	case api.CustomizedFieldType_Enumeration:
		return convertEnumer(fd, fv)
	case api.CustomizedFieldType_File:
		return &fieldvalue.FieldValue{
			File: fv.FileValue.convert(),
		}
	case api.CustomizedFieldType_Image:
		return &fieldvalue.FieldValue{
			Image: fv.ImageValue.convert(),
		}
	case api.CustomizedFieldType_Video:
		return &fieldvalue.FieldValue{
			Video: fv.VideoValue.convert(),
		}
	case api.CustomizedFieldType_Color:
		return &fieldvalue.FieldValue{
			Text: fv.ColorValue.convert(),
		}
	case api.CustomizedFieldType_MaterialReference:
		return &fieldvalue.FieldValue{
			Reference: fv.ReferenceValue.convert(),
		}
	default:
		return nil
	}
}

func convertEnumer(fd *api.Field, fv *FieldValue) *fieldvalue.FieldValue {
	e := fd.GetEnumeration()
	et := e.ValueType
	switch et {
	case 1: // int
		if e.IsMultiple {
			return &fieldvalue.FieldValue{
				NumberIntArray: fv.EnumValue.ConvertToMutilInt(),
			}
		}

		return &fieldvalue.FieldValue{
			NumberInt: fv.EnumValue.ConvertToSingleInt(),
		}
	case 2: // bool
		return &fieldvalue.FieldValue{
			Bool: fv.EnumValue.ConvertToBool(),
		}
	case 3: // 浮点
		if e.IsMultiple {
			return &fieldvalue.FieldValue{
				NumberFloatArray: fv.EnumValue.ConvertToMutilFloat(),
			}
		}

		return &fieldvalue.FieldValue{
			NumberFloat: fv.EnumValue.ConvertToSingleFloat(),
		}
	default: // 字符串
		if e.IsMultiple {
			return &fieldvalue.FieldValue{
				TextArray: fv.EnumValue.ConvertToMutilString(),
			}
		}

		return &fieldvalue.FieldValue{
			Text: fv.EnumValue.ConvertToSingleString(),
		}
	}
}

type OldMaterial struct {
	ID                 primitive.ObjectID   `bson:"_id" json:"id"`
	Scope              string               `bson:"scope"`
	TypeID             string               `bson:"typeID"` // 素材类型
	Name               string               `bson:"name"`
	IsVIP              int                  `bson:"isVIP"`
	TagID              []string             `bson:"tagIDs"`   // 用户使用的标签id，只存标签组末级的id 可以使用多个
	Platform           []string             `bson:"platform"` // 适用平台
	DatePeriod         DatePeriod           `bson:"datePeriod"`
	SystemAttributes   []*CustomAttribute   `bson:"systemAttributes"`
	CustomAttributes   []*CustomAttribute   `bson:"customAttributes"`
	LocalizeAttributes []*LocalizeAttribute `bson:"localizeAttributes"`
	Status             string               `bson:"status"`
	IsDeleted          bool                 `bson:"isDeleted"`
	Creator            string               `bson:"creator"`
	Mender             string               `bson:"mender"`
	CreatedAt          time.Time            `bson:"createdAt"`
	UpdatedAt          time.Time            `bson:"updatedAt"`
}

func (om *OldMaterial) convert(fd *api.FieldsDefinition) *Material {
	if om == nil {
		return nil
	}

	m := &Material{
		ID:        om.ID,
		Scope:     om.Scope,
		TypeID:    om.TypeID,
		IsDeleted: om.IsDeleted,
	}

	mv := MaterialVersion{
		VersionID:     primitive.NewObjectID(),
		VersionName:   "初始版本",
		Name:          om.Name,
		Vip:           om.IsVIP,
		Tag:           om.TagID,
		Platform:      om.Platform,
		ClientVersion: VersionFromSystemAttributes(om.SystemAttributes),
		ValidDuration: fieldvalue.Period{
			Begin: om.DatePeriod.StartDate,
			End:   om.DatePeriod.EndDate,
		},
		CreatedAt: om.CreatedAt,
		UpdatedAt: om.UpdatedAt,
		Creator:   om.Creator,
		Mender:    om.Mender,
		Status:    om.Status,
	}

	mv.Custom = convertCustomAttribuite(fd, om.CustomAttributes)
	mv.Localize = convertLocalizedAttribute(fd, om.LocalizeAttributes)
	m.Versions = append(m.Versions, mv)

	return m
}

type OldCategory struct {
	ID                 primitive.ObjectID   `bson:"_id" json:"id"`
	Scope              string               `bson:"scope"`
	Name               string               `bson:"name"`
	ParentID           string               `bson:"parentId"`
	TypeID             string               `bson:"typeID"`
	SortOrder          int                  `bson:"sortOrder"`
	SystemAttributes   []*CustomAttribute   `bson:"systemAttributes"`
	LocalizeAttributes []*LocalizeAttribute `bson:"localizeAttributes"`
	CustomAttributes   []*CustomAttribute   `bson:"customAttributes"`
	IsDeleted          bool                 `bson:"isDeleted"`
	Creator            string               `bson:"creator"`
	Mender             string               `bson:"mender"`
	CreatedAt          time.Time            `bson:"createdAt"`
	UpdatedAt          time.Time            `bson:"updatedAt"`
}

func (oc *OldCategory) convert(fd *api.FieldsDefinition) *Category {
	if oc == nil {
		return nil
	}

	res := &Category{
		ID:        oc.ID,
		Scope:     oc.Scope,
		Parent:    oc.ParentID,
		TypeID:    oc.TypeID,
		IsDeleted: oc.IsDeleted,
		SortOrder: oc.SortOrder,
	}

	cv := CategoryVersion{
		VersionID:   primitive.NewObjectID(),
		VersionName: "默认版本",
		Name:        oc.Name,
		Creator:     oc.Creator,
		Mender:      oc.Mender,
		CreatedAt:   oc.CreatedAt,
		UpdatedAt:   oc.UpdatedAt,
	}

	cv.Custom = convertCustomAttribuite(fd, oc.CustomAttributes)
	cv.Localize = convertLocalizedAttribute(fd, oc.LocalizeAttributes)

	res.Versions = append(res.Versions, cv)

	return res
}

type Category struct {
	ID        primitive.ObjectID `bson:"_id" json:"id"`
	Scope     string             `bson:"scope"`
	Parent    string             `bson:"parent"`
	TypeID    string             `bson:"typeID"`
	SortOrder int                `bson:"sortOrder"`
	IsDeleted bool               `bson:"isDeleted"`
	Versions  []CategoryVersion  `bson:"versions"`
}

type CategoryVersion struct {
	VersionID   primitive.ObjectID                `bson:"versionId"`
	VersionName string                            `bson:"versionName"`
	Name        string                            `bson:"name"`
	Creator     string                            `bson:"creator"`
	Mender      string                            `bson:"mender"`
	CreatedAt   time.Time                         `bson:"createdAt"`
	UpdatedAt   time.Time                         `bson:"updatedAt"`
	Custom      map[string]*fieldvalue.FieldValue `bson:"custom"`
	Localize    []*fieldvalue.Localize            `bson:"localize"`
}

type Material struct {
	ID        primitive.ObjectID `bson:"_id",inline"`
	Scope     string             `bson:"scope"`
	TypeID    string             `bson:"typeID"` // 素材类型
	IsDeleted bool               `bson:"isDeleted"`
	Versions  []MaterialVersion  `bson:"versions"` // 素材多版本
}

func (m *Material) addVersion(mv MaterialVersion) {
	m.Versions = append(m.Versions, mv)
}

func (m *Material) getVersionIDByKey(key string) (string, bool) {
	for i, v := range m.Versions {
		mvkey := v.getKey(m.ID.Hex())
		if mvkey == key || strings.HasPrefix(mvkey, key) {
			if i == 0 {
				return "", true
			}

			return v.VersionID.Hex(), true
		}
	}

	return "", false
}

type MaterialVersion struct {
	VersionID     primitive.ObjectID                `bson:"versionID"`
	VersionName   string                            `bson:"versionName"`
	Name          string                            `bson:"name"`
	Vip           int                               `bson:"vip"`
	Tag           []string                          `bson:"tag"`      // 用户使用的标签id，只存标签组末级的id 可以使用多个
	Platform      []string                          `bson:"platform"` // 适用平台
	ClientVersion fieldvalue.Version                `bson:"clientVersion"`
	ValidDuration fieldvalue.Period                 `bson:"validDuration"`
	CreatedAt     time.Time                         `bson:"createdAt"`
	UpdatedAt     time.Time                         `bson:"updatedAt"`
	Creator       string                            `bson:"creator"`
	Mender        string                            `bson:"mender"`
	Status        string                            `bson:"status"`
	Custom        map[string]*fieldvalue.FieldValue `bson:"custom"`
	Localize      []*fieldvalue.Localize            `bson:"localize"`
}

func (m *MaterialVersion) getKey(id string) string {
	key := id
	if m.Vip > 0 {
		key = fmt.Sprintf("%s%t", key, true)
	} else {
		key = fmt.Sprintf("%s%t", key, false)
	}

	key = fmt.Sprintf("%s%d%d", key, m.ValidDuration.Begin, m.ValidDuration.End)

	return key
}

func (mv *MaterialVersion) GetLocalize() []map[string]*fieldvalue.FieldValue {
	res := make([]map[string]*fieldvalue.FieldValue, 0, len(mv.Localize))
	for i := range mv.Localize {
		loc := mv.Localize[i]
		res = append(res, loc.GetLocalize())
	}

	return res
}

func (mv *MaterialVersion) GetBase() map[string]*fieldvalue.FieldValue {
	res := make(map[string]*fieldvalue.FieldValue)

	res[pkg.Code_Name] = fieldvalue.TextFieldValue(mv.Name)

	parr := []string{}
	for _, v := range mv.Platform {
		parr = append(parr, string(v))
	}
	res[pkg.Code_Platform] = &fieldvalue.FieldValue{
		TextArray: parr,
	}

	res[pkg.Code_ValidDuration] = &fieldvalue.FieldValue{
		Period: &mv.ValidDuration,
	}

	res[pkg.Code_ClientVersion] = &fieldvalue.FieldValue{
		Version: &mv.ClientVersion,
	}

	res[pkg.Code_Tag] = &fieldvalue.FieldValue{
		TextArray: mv.Tag,
	}

	vip := int64(mv.Vip)
	res[pkg.Code_VIP] = &fieldvalue.FieldValue{
		NumberInt: &vip,
	}

	for k, v := range mv.Custom {
		res[k] = v
	}

	return res
}

func getFieldByCode(code string, fd *api.FieldsDefinition) *api.Field {
	if fd == nil {
		return nil
	}

	for i := range fd.Fields {
		if fd.Fields[i].Code == code {
			return fd.Fields[i]
		}
	}

	return nil
}

func convertCustomAttribuite(fd *api.FieldsDefinition, attr []*CustomAttribute) map[string]*fieldvalue.FieldValue {
	res := make(map[string]*fieldvalue.FieldValue)
	if fd == nil {
		return res
	}

	for _, v := range attr {
		if v == nil {
			continue
		}

		field := getFieldByCode(v.Code, fd)
		res[v.Code] = v.convert(field)
	}

	return res
}

func convertLocalizedAttribute(fd *api.FieldsDefinition, locAttr []*LocalizeAttribute) []*fieldvalue.Localize {
	res := make([]*fieldvalue.Localize, 0, len(locAttr))
	for _, v := range locAttr {
		res = append(res, v.convert(fd))
	}

	return res
}

func VersionFromSystemAttributes(sysAttr []*CustomAttribute) fieldvalue.Version {
	for _, v := range sysAttr {
		if v.Code == pkg.Code_ClientVersion && v.Value != nil {
			return v.Value.VersionValue.convert()
		}
	}

	return fieldvalue.Version{}
}

type (
	FieldCategory int

	SystemFieldType int

	CustomizedFieldType int
)

const (
	FieldCategoryMaterial            FieldCategory = 1 // 素材
	FieldCategoryMaterialCate        FieldCategory = 2 // 素材分类
	FieldCategoryOperationalActivity FieldCategory = 3 // 运营活动
	FieldCategoryHTML5               FieldCategory = 4 // h5
)

const (
	SystemFieldName SystemFieldType = iota + 1
	SystemFieldValidDuration
	SystemFieldPlatform
	SystemFieldVIP
	SystemFieldClientVersion
	SystemFieldLocale
	SystemFieldPriority
	SystemFieldParent
	SystemFieldTag
)

const (
	CustomizedFieldText CustomizedFieldType = iota + 1
	CustomizedFieldNumberRange
	CustomizedFieldDatetime
	CustomizedFieldEnumeration
	CustomizedFieldImage
	CustomizedFieldFile
	CustomizedFieldMaterialReference
	CustomizedVideo
	CustomizedGoto
	CustomizedTag
	CustomizedNumber
	CustomizedColor
)

type Field struct {
	Code                string              `bson:"code"`
	Name                string              `bson:"name"`
	Comment             string              `bson:"comment"`
	Required            bool                `bson:"required"`
	IsSystematized      bool                `bson:"isSystematized"`
	IsLocalized         bool                `bson:"isLocalized"`
	SysFieldType        SystemFieldType     `bson:"sysFieldType"`
	CustomizedFieldType CustomizedFieldType `bson:"customizedFieldType"`

	// 自定义字段配置
	TextField              *CustomizedTextConfig              `bson:"textField,omitempty"`
	NumberRangeField       *CustomizedNumberRangeConfig       `bson:"numberRangeField,omitempty"`
	DatetimeField          *CustomizedDatetimeConfig          `bson:"datetimeField,omitempty"`
	EnumField              *CustomizedEnumerationConfig       `bson:"enumField,omitempty"`
	FileField              *CustomizedFileConfig              `bson:"fileField,omitempty"`
	ImageField             *CustomizedImageConfig             `bson:"imageField,omitempty"`
	MaterialReferenceField *CustomizedMaterialReferenceConfig `bson:"materialReferenceField,omitempty"`
	VideoField             *CustomizedVideoConfig             `bson:"videoField,omitempty"`
	GotoField              *CustomizedGotoConfig              `bson:"gotoField,omitempty"`
	TagField               *CustomizedTagConfig               `bson:"tagField,omitempty"`
	NumberField            *CustomizedNumberConfig            `bson:"numberField,omitempty"`
	ColorField             *CustomizedColorConfig             `bson:"colorField,omitempty"`
}

type GotoParamName struct {
	IOS     string `json:"iOS"`
	Android string `json:"android"`
}

type DefinitionConfigs struct {
	SystemFieldNameIsUnique bool           `bson:"systemFieldNameIsUnique"`
	CoverCode               string         `bson:"coverCode"`
	ChildRefCode            string         `bson:"childRefCode"` // 子节点关联字段名,如素材包>素材
	PackageCodes            []string       `bson:"packageCode"`  // 支持的打包字段
	GotoParam               *GotoParamName `bson:"gotoParam"`    // goto参数设置
}

type FieldsDefinition struct {
	ID     string        `bson:"_id"`
	Scope  string        `bson:"scope"`
	Name   string        `bson:"name"`
	Type   FieldCategory `bson:"type"`
	Fields []*Field      `bson:"fields"`

	Configs DefinitionConfigs `bson:"configs"`

	DeletedAt int64 `bson:"deletedAt"`
}

type (
	CustomizedTextType int

	CustomizedTextConfig struct {
		Type CustomizedTextType `bson:"type"`
		Hint string             `bson:"hint"`
	}
)

const (
	CustomizedTextSingle CustomizedTextType = 1
	CustomizedTextMulti  CustomizedTextType = 2
)

type CustomizedNumberRangeConfig struct{}

type (
	CustomizedDatetimeType int

	CustomizedDatetimeConfig struct {
		Type CustomizedDatetimeType `bson:"type"`
	}
)

const (
	CustomizedDatetimePoint CustomizedDatetimeType = 1
	CustomizedDatetimeRange CustomizedDatetimeType = 2
)

const (
	CustomizedNumberTypeInt   CustomizedNumberType = 1
	CustomizedNumberTypeFloat CustomizedNumberType = 2
)

const (
	CustomizedEnumerationValueTypeString = 0
	CustomizedEnumerationValueTypeInt    = 1
	CustomizedEnumerationValueTypeBool   = 2
	CustomizedEnumerationValueTypeFloat  = 3
)

type (
	CustomizedEnumerationConfig struct {
		IsMultiple bool                         `bson:"isMultiple"`
		ValueType  int                          `bson:"valueType"`
		EnumItems  []*CustomizedEnumerationItem `bson:"enumItems"`
	}

	CustomizedEnumerationItem struct {
		Label string `bson:"label"`
		Value string `bson:"value"`
	}
)

type CustomizedFileConfig struct{}

type (
	CustomizedImageConfig struct {
		SupportedSize []*ImageSize `bson:"supportedSize"`
	}

	ImageSize struct {
		Width  int `bson:"width"`
		Height int `bson:"height"`
	}
)

type CustomizedMaterialReferenceConfig struct {
	ID     string `bson:"id"`
	IsPack bool   `bson:"isPack"`
}

type (
	CustomizedTagConfig struct {
		TagType string `bson:"tagType"`
	}
)

type (
	CustomizedNumberType   int32
	CustomizedNumberConfig struct {
		Type CustomizedNumberType `bson:"type"`
		Hint string               `bson:"hint"`
	}
)

type (
	CustomizedColorConfig struct{}
)

type (
	CustomizedVideoConfig struct{}

	CustomizedGotoConfig struct {
		EnabledTypes  []string // 可用的类型: text: 直接文本框输入, material: 素材
		MaterialCodes []string // 启用的素材类型, 如果没有则所有素材都启用
	}
)

// SyncRecoder 同步数据的记录
type SyncRecoder struct {
	DBFullColl string
	Type       int
	ID         string
	Scope      string
	Env        string
	Err        string
}

func NewSyncRecoder(dbfc, id, err, scope, env string, tp int) *SyncRecoder {
	return &SyncRecoder{
		DBFullColl: dbfc,
		Type:       tp,
		ID:         id,
		Err:        err,
		Scope:      scope,
		Env:        env,
	}
}

type FindOptions struct {
	ldao.MongodbFindOptions
}

func (opt *FindOptions) Filter() (interface{}, error) {
	return primitive.M{}, nil
}

func (opt *FindOptions) Sorts() ldao.SortFields {
	// 去除lint警告而已,后续通过lint检测配置处理
	_ = opt

	return ldao.SortFields{
		{
			Name:      "_id",
			Direction: ldao.SortFieldDesc,
		},
	}
}

type MaterialPosition struct {
	ID               primitive.ObjectID                `bson:"_id,omitempty"`
	Scope            string                            `bson:"scope"`
	Code             string                            `bson:"code"` // 客户端请求标识，同一 scope，platform 下不可重复
	Name             string                            `bson:"name"`
	MaterialTypeID   string                            `bson:"materialTypeId"` // 字段表配置ID
	ContainsCategory bool                              `bson:"containsCategory"`
	Platform         []string                          `bson:"platform"`
	ClientVersion    map[string]*version.ClientVersion `bson:"clientVersion"` // optional
	DeviceLimited    map[DeviceLimitedType]string      `bson:"deviceLimited"`
	Summary          string                            `bson:"summary"`
	Active           bool                              `bson:"active"`
	GotoId           []string                          `bson:"gotoId"` // goto配置id
	IsDeleted        bool                              `bson:"isDeleted"`
	EditInfo         `bson:",inline"`
}

type EditInfo struct {
	CreatedAt time.Time
	UpdatedAt time.Time
	Creator   string
	Editor    string
}

type (
	DeviceLimitedType string
	ID                = primitive.ObjectID
	PlanType          string
)

const (
	DeviceLimitedModel         DeviceLimitedType = "model"
	DeviceLimitedSystemVersion DeviceLimitedType = "systemVersion"
)

type Plan struct {
	ID                    ID                `bson:"_id,omitempty"`
	PosID                 ID                `bson:"posID"`
	Scope                 string            `bson:"scope"`
	Type                  PlanType          `bson:"type"`
	Name                  string            `bson:"name"`
	Active                bool              `bson:"active"`
	Period                Period            `bson:",inline"`
	Priority              uint              `bson:"priority"`
	UserSelection         UserSelection     `bson:"userSelection"`
	PlacingContent        []*PlacingContent `bson:"placingContents"`
	DecisionDataChangedAt time.Time         `bson:"decisionDataChangedAt"`
	IsDeleted             bool              `bson:"isDeleted"`
	*EditInfo             `bson:",inline"`
}

var cacheOverrideVersionID = make(map[string]string)

func (p *Plan) newOverrideMaterialsVersion(mdb dao.MongodbDAO, scope, env string) (bool, error) {
	if p.PlacingContent == nil {
		return false, nil
	}
	hasOverride := false
	for _, v := range p.PlacingContent {
		if v.Categories != nil {
			for _, c := range v.Categories {
				if c.Materials != nil {
					for _, m := range c.Materials {
						if m.hasOverride() {
							if isExcludeMaterialsID(scope, env, m.ID) {
								continue
							}
							key := m.getCacheKey()
							material, err := getSingleNewMaterial(context.Background(), m.ID, mdb)
							if err != nil {
								return false, err
							}

							vid, find := material.getVersionIDByKey(key)
							if find {
								if vid != "" {
									m.VersionID = vid
									hasOverride = true
									cacheOverrideVersionID[key] = vid
								}
								continue
							}

							// 需要生成新的素材版本支持覆盖数据
							defv := material.Versions[0]
							if versionID, ok := cacheOverrideVersionID[key]; ok {
								ovid, err := primitive.ObjectIDFromHex(versionID)
								if err != nil {
									return false, err
								}

								defv.VersionID = ovid
							} else {
								defv.VersionID = primitive.NewObjectID()
							}

							if m.VIP != nil {
								if *m.VIP == true {
									defv.Vip = 1
								} else {
									defv.Vip = 0
								}
							}

							if m.Period != nil {
								defv.ValidDuration.Begin = m.Period.Begin.Unix()
								defv.ValidDuration.End = m.Period.End.Unix()
							}

							defv.VersionName = fmt.Sprintf("覆盖版本%d", len(material.Versions))
							material.addVersion(defv)
							// TODO: 更新素材
							if _, err := mdb.Collection().UpdateOne(
								context.Background(), primitive.M{"_id": material.ID}, op.Set(material),
								options.Update().SetUpsert(true),
							); err != nil {
								return false, err
							}
							fmt.Printf("%s,%s 新增了覆盖版本", material.ID.Hex(), defv.VersionID.Hex())

							m.VersionID = defv.VersionID.Hex()
							hasOverride = true
							cacheOverrideVersionID[key] = m.VersionID
						}
					}
				}
			}
		}

		if v.Materials != nil {
			for _, m := range v.Materials {
				if m.hasOverride() {
					key := m.getCacheKey()
					material, err := getSingleNewMaterial(context.Background(), m.ID, mdb)
					if err != nil {
						return false, err
					}

					vid, find := material.getVersionIDByKey(key)
					if find {
						if vid != "" {
							m.VersionID = vid
							hasOverride = true
							cacheOverrideVersionID[key] = vid
						}
						continue
					}

					// 需要生成新的素材版本支持覆盖数据
					defv := material.Versions[0]
					if versionID, ok := cacheOverrideVersionID[key]; ok {
						ovid, err := primitive.ObjectIDFromHex(versionID)
						if err != nil {
							return false, err
						}

						defv.VersionID = ovid
					} else {
						defv.VersionID = primitive.NewObjectID()
					}

					if m.VIP != nil {
						if *m.VIP == true {
							defv.Vip = 1
						} else {
							defv.Vip = 0
						}
					}

					if m.Period != nil {
						defv.ValidDuration.Begin = m.Period.Begin.Unix()
						defv.ValidDuration.End = m.Period.End.Unix()
					}

					defv.VersionName = fmt.Sprintf("覆盖版本%d", len(material.Versions))
					material.addVersion(defv)
					// TODO: 更新素材
					if _, err := mdb.Collection().UpdateOne(
						context.Background(), primitive.M{"_id": material.ID}, op.Set(material),
						options.Update().SetUpsert(true),
					); err != nil {
						return false, err
					}
					fmt.Printf("%s,%s 新增了覆盖版本", material.ID.Hex(), defv.VersionID.Hex())

					m.VersionID = defv.VersionID.Hex()
					hasOverride = true
					cacheOverrideVersionID[key] = vid
				}
			}
		}
	}

	return hasOverride, nil
}

type UserSelectionType uint8

type UserSelection struct {
	Type UserSelectionType `bson:"type"`

	// 用户属性，仅当 type 为 UserProperties 时有效
	UserProperties []*UserSelectionProperty `bson:"userProperties"`
	// 用户分群ID，仅当 type 为 UserGroups 时有效
	UserGroupsID []string `bson:"userGroupsID"`
}

type UserSelectionProperty struct {
	Code  string   `bson:"code"`
	Op    string   `bson:"op"`
	Value []string `bson:"value"`
}

type PlacingContent struct {
	ID    ID    `bson:"id"`
	Ratio uint8 `bson:"ratio"`
	// 当存在分类时
	CategoryTypeDefID string             `bson:"categoryTypeDefID,omitempty"` // 分类所使用的字段表
	Categories        []*PlacingCategory `bson:"categories,omitempty"`

	// 当该素材位不存在分类时，仅有素材
	Materials []*PlacingMaterial `bson:"materials,omitempty"`
}

type PlacingCategory struct {
	ID        string             `bson:"id"`
	PID       string             `bson:"pid,omitempty"`
	Active    *bool              `bson:"active,omitempty"`
	Period    *Period            `bson:"period,omitempty"`
	Materials []*PlacingMaterial `bson:"materials"`
}

type PlacingMaterial struct {
	ID        string  `bson:"id"`
	VersionID string  `bson:"versionID"` // 投放的素材版本，默认版本为空字符串
	VIP       *bool   `bson:"vip"`
	Period    *Period `bson:"period,omitempty"`
}

func (pm *PlacingMaterial) hasOverride() bool {
	if pm.VersionID != "" {
		return false
	}

	if pm.VIP != nil {
		return true
	}

	if pm.Period != nil {
		return true
	}

	return false
}

func (pm *PlacingMaterial) getCacheKey() string {
	key := pm.ID
	if pm.VIP != nil {
		key = fmt.Sprintf("%s%t", key, *pm.VIP)
	}

	if pm.Period != nil {
		key = fmt.Sprintf("%s%d%d", key, pm.Period.Begin.Unix(), pm.Period.End.Unix())
	}

	return key
}

func (pm *PlacingMaterial) setVersionID(versionID string) {
	pm.VersionID = versionID
}

type Period struct {
	Begin time.Time `bson:"begin,omitempty"`
	End   time.Time `bson:"end,omitempty"`
}

type H5PropertiesWithActName struct {
	ID        string
	Attribute string
	Style     string
	ActID     string
	ActName   string
}

const (
	DefaultUserSelection UserSelectionType = iota
	UserProperties
	UserGroups
)

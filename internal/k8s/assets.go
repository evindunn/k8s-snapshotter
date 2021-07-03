package k8s

import (
	_ "embed"
	"k8s.io/apimachinery/pkg/util/yaml"
	"strings"
	"text/template"
)

/*
yamlDecodeBufferBytes determines how far the k8s yaml parser will read into a stream to
determine whether it's valid yaml
*/
const yamlDecodeBufferBytes = 16

/*
createdByLabelName See https://kubernetes.io/docs/concepts/overview/working-with-objects/common-labels/
*/
const createdByLabelName = "app.kubernetes.io/created-by"

/*
createdByLabelValue helps to filter out PersistentVolumeClaims created by this app. It's added to
all created PersistentVolumeClaims, and any PersistentVolumeClaim with the label is skipped when
backups are performed
 */
const createdByLabelValue = "com.github.evindunn.k8s-snapshotter"

type SnapshotValues struct {
	SnapshotName	string
	Namespace 		string
	PVCName 		string
}

type JobTemplateValues struct {
	JobName         string
	JobLabels		map[string]string
	Namespace		string
	PVCName 		string
}

type PVCFromSnapshotValues struct {
	SnapshotName 	string
	PVCLabels		map[string]string
	PVCSize			string
	Namespace 		string
}

//go:embed assets/volumeSnapshot.goyml
var SnapshotTemplate string

//go:embed assets/job.goyml
var JobTemplate string

//go:embed assets/volumeFromSnapshot.goyml
var VolumeFromSnapshotTemplate string

/*
ParseK8STemplate templates templateStr, assigns it templateName, and decodes it with templateValues
into a kubernetes object, decodeInto
 */
func ParseK8STemplate(templateStr string, templateName string, templateValues interface{}, decodeInto interface{}) error {
	var resourceWriter strings.Builder

	jobTemplate, err := template.New(templateName).Parse(templateStr)
	if err != nil {
		return err
	}

	err = jobTemplate.Execute(&resourceWriter, templateValues)
	if err != nil {
		return err
	}

	resourceStr := resourceWriter.String()

	yamlDecoder := yaml.NewYAMLOrJSONDecoder(
		strings.NewReader(resourceStr),
		yamlDecodeBufferBytes,
	)

	err = yamlDecoder.Decode(decodeInto)
	if err != nil {
		return err
	}

	return nil
}
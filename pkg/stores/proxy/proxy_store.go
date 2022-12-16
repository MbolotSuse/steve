package proxy

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"reflect"
	"regexp"
	"strconv"

	"github.com/pkg/errors"
	"github.com/rancher/apiserver/pkg/types"
	"github.com/rancher/steve/pkg/accesscontrol"
	"github.com/rancher/steve/pkg/attributes"
	metricsStore "github.com/rancher/steve/pkg/stores/metrics"
	"github.com/rancher/steve/pkg/stores/partition"
	"github.com/rancher/wrangler/pkg/data"
	"github.com/rancher/wrangler/pkg/schemas/validation"
	"github.com/rancher/wrangler/pkg/summary"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	apitypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

const watchTimeoutEnv = "CATTLE_WATCH_TIMEOUT_SECONDS"

var (
	lowerChars  = regexp.MustCompile("[a-z]+")
	paramScheme = runtime.NewScheme()
	paramCodec  = runtime.NewParameterCodec(paramScheme)
)

func init() {
	metav1.AddToGroupVersion(paramScheme, metav1.SchemeGroupVersion)
}

// ClientGetter is a dynamic kubernetes client factory.
type ClientGetter interface {
	IsImpersonating() bool
	K8sInterface(ctx *types.APIRequest) (kubernetes.Interface, error)
	AdminK8sInterface() (kubernetes.Interface, error)
	Client(ctx *types.APIRequest, schema *types.APISchema, namespace string, warningHandler rest.WarningHandler) (dynamic.ResourceInterface, error)
	DynamicClient(ctx *types.APIRequest, warningHandler rest.WarningHandler) (dynamic.Interface, error)
	AdminClient(ctx *types.APIRequest, schema *types.APISchema, namespace string, warningHandler rest.WarningHandler) (dynamic.ResourceInterface, error)
	TableClient(ctx *types.APIRequest, schema *types.APISchema, namespace string, warningHandler rest.WarningHandler) (dynamic.ResourceInterface, error)
	TableAdminClient(ctx *types.APIRequest, schema *types.APISchema, namespace string, warningHandler rest.WarningHandler) (dynamic.ResourceInterface, error)
	TableClientForWatch(ctx *types.APIRequest, schema *types.APISchema, namespace string, warningHandler rest.WarningHandler) (dynamic.ResourceInterface, error)
	TableAdminClientForWatch(ctx *types.APIRequest, schema *types.APISchema, namespace string, warningHandler rest.WarningHandler) (dynamic.ResourceInterface, error)
}

type WarningBuffer struct {
	Warnings []types.Warning
}

func (w *WarningBuffer) HandleWarningHeader(code int, agent string, text string) {
	if w.Warnings == nil {
		w.Warnings = []types.Warning{}
	}
	w.Warnings = append(w.Warnings, types.Warning{
		Code:  code,
		Agent: agent,
		Text:  text,
	})
}

// RelationshipNotifier is an interface for handling wrangler summary.Relationship events.
type RelationshipNotifier interface {
	OnInboundRelationshipChange(ctx context.Context, schema *types.APISchema, namespace string) <-chan *summary.Relationship
}

// Store implements types.Store directly on top of kubernetes.
type Store struct {
	clientGetter ClientGetter
	notifier     RelationshipNotifier
}

// NewProxyStore returns a wrapped types.Store.
func NewProxyStore(clientGetter ClientGetter, notifier RelationshipNotifier, lookup accesscontrol.AccessSetLookup) types.Store {
	return &errorStore{
		Store: &WatchRefresh{
			Store: &partition.Store{
				Partitioner: &rbacPartitioner{
					proxyStore: &Store{
						clientGetter: clientGetter,
						notifier:     notifier,
					},
				},
			},
			asl: lookup,
		},
	}
}

// ByID looks up a single object by its ID.
func (s *Store) ByID(apiOp *types.APIRequest, schema *types.APISchema, id string) (types.APIObject, error) {
	result, warnings, err := s.byID(apiOp, schema, apiOp.Namespace, id)
	return toAPI(schema, result, warnings), err
}

func decodeParams(apiOp *types.APIRequest, target runtime.Object) error {
	return paramCodec.DecodeParameters(apiOp.Request.URL.Query(), metav1.SchemeGroupVersion, target)
}

func toAPI(schema *types.APISchema, obj runtime.Object, warnings []types.Warning) types.APIObject {
	if obj == nil || reflect.ValueOf(obj).IsNil() {
		return types.APIObject{}
	}

	if unstr, ok := obj.(*unstructured.Unstructured); ok {
		obj = moveToUnderscore(unstr)
	}

	apiObject := types.APIObject{
		Type:   schema.ID,
		Object: obj,
	}

	m, err := meta.Accessor(obj)
	if err != nil {
		return apiObject
	}

	id := m.GetName()
	ns := m.GetNamespace()
	if ns != "" {
		id = fmt.Sprintf("%s/%s", ns, id)
	}

	apiObject.ID = id
	apiObject.Warnings = warnings
	return apiObject
}

func (s *Store) byID(apiOp *types.APIRequest, schema *types.APISchema, namespace, id string) (*unstructured.Unstructured, []types.Warning, error) {
	buffer := &WarningBuffer{}
	k8sClient, err := metricsStore.Wrap(s.clientGetter.TableClient(apiOp, schema, namespace, buffer))
	if err != nil {
		return nil, nil, err
	}

	opts := metav1.GetOptions{}
	if err := decodeParams(apiOp, &opts); err != nil {
		return nil, nil, err
	}

	obj, err := k8sClient.Get(apiOp, id, opts)
	rowToObject(obj)
	return obj, buffer.Warnings, err
}

func moveFromUnderscore(obj map[string]interface{}) map[string]interface{} {
	if obj == nil {
		return nil
	}
	for k := range types.ReservedFields {
		v, ok := obj["_"+k]
		delete(obj, "_"+k)
		delete(obj, k)
		if ok {
			obj[k] = v
		}
	}
	return obj
}

func moveToUnderscore(obj *unstructured.Unstructured) *unstructured.Unstructured {
	if obj == nil {
		return nil
	}

	for k := range types.ReservedFields {
		v, ok := obj.Object[k]
		if ok {
			delete(obj.Object, k)
			obj.Object["_"+k] = v
		}
	}

	return obj
}

func rowToObject(obj *unstructured.Unstructured) {
	if obj == nil {
		return
	}
	if obj.Object["kind"] != "Table" ||
		(obj.Object["apiVersion"] != "meta.k8s.io/v1" &&
			obj.Object["apiVersion"] != "meta.k8s.io/v1beta1") {
		return
	}

	items := tableToObjects(obj.Object)
	if len(items) == 1 {
		obj.Object = items[0].Object
	}
}

func tableToList(obj *unstructured.UnstructuredList) {
	if obj.Object["kind"] != "Table" ||
		(obj.Object["apiVersion"] != "meta.k8s.io/v1" &&
			obj.Object["apiVersion"] != "meta.k8s.io/v1beta1") {
		return
	}

	obj.Items = tableToObjects(obj.Object)
}

func tableToObjects(obj map[string]interface{}) []unstructured.Unstructured {
	var result []unstructured.Unstructured

	rows, _ := obj["rows"].([]interface{})
	for _, row := range rows {
		m, ok := row.(map[string]interface{})
		if !ok {
			continue
		}
		cells := m["cells"]
		object, ok := m["object"].(map[string]interface{})
		if !ok {
			continue
		}

		data.PutValue(object, cells, "metadata", "fields")
		result = append(result, unstructured.Unstructured{
			Object: object,
		})
	}

	return result
}

// ByNames filters a list of objects by an allowed set of names.
// In plain kubernetes, if a user has permission to 'list' or 'watch' a defined set of resource names,
// performing the list or watch will result in a Forbidden error, because the user does not have permission
// to list *all* resources.
// With this filter, the request can be performed successfully, and only the allowed resources will
// be returned in the list.
func (s *Store) ByNames(apiOp *types.APIRequest, schema *types.APISchema, names sets.String) (types.APIObjectList, error) {
	if apiOp.Namespace == "*" {
		// This happens when you grant namespaced objects with "get" by name in a clusterrolebinding. We will treat
		// this as an invalid situation instead of listing all objects in the cluster and filtering by name.
		return types.APIObjectList{}, nil
	}

	buffer := &WarningBuffer{}
	adminClient, err := s.clientGetter.TableAdminClient(apiOp, schema, apiOp.Namespace, buffer)
	if err != nil {
		return types.APIObjectList{}, err
	}

	objs, err := s.list(apiOp, schema, adminClient, buffer)
	if err != nil {
		return types.APIObjectList{}, err
	}

	var filtered []types.APIObject
	for _, obj := range objs.Objects {
		if names.Has(obj.Name()) {
			filtered = append(filtered, obj)
		}
	}

	objs.Objects = filtered
	return objs, nil
}

// List returns a list of resources.
func (s *Store) List(apiOp *types.APIRequest, schema *types.APISchema) (types.APIObjectList, error) {
	buffer := &WarningBuffer{}
	client, err := s.clientGetter.TableClient(apiOp, schema, apiOp.Namespace, buffer)
	if err != nil {
		return types.APIObjectList{}, err
	}
	return s.list(apiOp, schema, client, buffer)
}

func (s *Store) list(apiOp *types.APIRequest, schema *types.APISchema, client dynamic.ResourceInterface, buffer *WarningBuffer) (types.APIObjectList, error) {
	opts := metav1.ListOptions{}
	if err := decodeParams(apiOp, &opts); err != nil {
		return types.APIObjectList{}, nil
	}

	k8sClient, _ := metricsStore.Wrap(client, nil)
	resultList, err := k8sClient.List(apiOp, opts)
	if err != nil {
		return types.APIObjectList{}, err
	}

	tableToList(resultList)

	result := types.APIObjectList{
		Revision: resultList.GetResourceVersion(),
		Continue: resultList.GetContinue(),
	}

	for i := range resultList.Items {
		result.Objects = append(result.Objects, toAPI(schema, &resultList.Items[i], buffer.Warnings))
	}

	return result, nil
}

func returnErr(err error, c chan types.APIEvent) {
	c <- types.APIEvent{
		Name:  "resource.error",
		Error: err,
	}
}

func (s *Store) listAndWatch(apiOp *types.APIRequest, client dynamic.ResourceInterface, schema *types.APISchema, w types.WatchRequest, result chan types.APIEvent) {
	rev := w.Revision
	if rev == "-1" || rev == "0" {
		rev = ""
	}

	timeout := int64(60 * 30)
	timeoutSetting := os.Getenv(watchTimeoutEnv)
	if timeoutSetting != "" {
		userSetTimeout, err := strconv.Atoi(timeoutSetting)
		if err != nil {
			logrus.Debugf("could not parse %s environment variable, error: %v", watchTimeoutEnv, err)
		} else {
			timeout = int64(userSetTimeout)
		}
	}
	k8sClient, _ := metricsStore.Wrap(client, nil)
	watcher, err := k8sClient.Watch(apiOp, metav1.ListOptions{
		Watch:           true,
		TimeoutSeconds:  &timeout,
		ResourceVersion: rev,
		LabelSelector:   w.Selector,
	})
	if err != nil {
		returnErr(errors.Wrapf(err, "stopping watch for %s: %v", schema.ID, err), result)
		return
	}
	defer watcher.Stop()
	logrus.Debugf("opening watcher for %s", schema.ID)

	eg, ctx := errgroup.WithContext(apiOp.Context())

	go func() {
		<-ctx.Done()
		watcher.Stop()
	}()

	if s.notifier != nil {
		eg.Go(func() error {
			for rel := range s.notifier.OnInboundRelationshipChange(ctx, schema, apiOp.Namespace) {
				obj, warnings, err := s.byID(apiOp, schema, rel.Namespace, rel.Name)
				if err == nil {
					result <- s.toAPIEvent(apiOp, schema, watch.Modified, obj, warnings)
				} else {
					logrus.Debugf("notifier watch error: %v", err)
					returnErr(errors.Wrapf(err, "notifier watch error: %v", err), result)
				}
			}
			return fmt.Errorf("closed")
		})
	}

	eg.Go(func() error {
		for event := range watcher.ResultChan() {
			if event.Type == watch.Error {
				if status, ok := event.Object.(*metav1.Status); ok {
					logrus.Debugf("event watch error: %s", status.Message)
					returnErr(fmt.Errorf("event watch error: %s", status.Message), result)
				} else {
					logrus.Debugf("event watch error: could not decode event object %T", event.Object)
				}
				continue
			}
			result <- s.toAPIEvent(apiOp, schema, event.Type, event.Object, nil)
		}
		return fmt.Errorf("closed")
	})

	_ = eg.Wait()
	return
}

// WatchNames returns a channel of events filtered by an allowed set of names.
// In plain kubernetes, if a user has permission to 'list' or 'watch' a defined set of resource names,
// performing the list or watch will result in a Forbidden error, because the user does not have permission
// to list *all* resources.
// With this filter, the request can be performed successfully, and only the allowed resources will
// be returned in watch.
func (s *Store) WatchNames(apiOp *types.APIRequest, schema *types.APISchema, w types.WatchRequest, names sets.String) (chan types.APIEvent, error) {
	buffer := &WarningBuffer{}
	adminClient, err := s.clientGetter.TableAdminClientForWatch(apiOp, schema, apiOp.Namespace, buffer)
	if err != nil {
		return nil, err
	}
	c, err := s.watch(apiOp, schema, w, adminClient)
	if err != nil {
		return nil, err
	}

	result := make(chan types.APIEvent)
	go func() {
		defer close(result)
		for item := range c {
			if item.Error == nil && names.Has(item.Object.Name()) {
				result <- item
			}
		}
	}()

	return result, nil
}

// Watch returns a channel of events for a list or resource.
func (s *Store) Watch(apiOp *types.APIRequest, schema *types.APISchema, w types.WatchRequest) (chan types.APIEvent, error) {
	buffer := &WarningBuffer{}
	client, err := s.clientGetter.TableClientForWatch(apiOp, schema, apiOp.Namespace, buffer)
	if err != nil {
		return nil, err
	}
	return s.watch(apiOp, schema, w, client)
}

func (s *Store) watch(apiOp *types.APIRequest, schema *types.APISchema, w types.WatchRequest, client dynamic.ResourceInterface) (chan types.APIEvent, error) {
	result := make(chan types.APIEvent)
	go func() {
		s.listAndWatch(apiOp, client, schema, w, result)
		logrus.Debugf("closing watcher for %s", schema.ID)
		close(result)
	}()
	return result, nil
}

func (s *Store) toAPIEvent(apiOp *types.APIRequest, schema *types.APISchema, et watch.EventType, obj runtime.Object, warnings []types.Warning) types.APIEvent {
	name := types.ChangeAPIEvent
	switch et {
	case watch.Deleted:
		name = types.RemoveAPIEvent
	case watch.Added:
		name = types.CreateAPIEvent
	}

	if unstr, ok := obj.(*unstructured.Unstructured); ok {
		rowToObject(unstr)
	}

	event := types.APIEvent{
		Name:   name,
		Object: toAPI(schema, obj, warnings),
	}

	m, err := meta.Accessor(obj)
	if err != nil {
		return event
	}

	event.Revision = m.GetResourceVersion()
	return event
}

// Create creates a single object in the store.
func (s *Store) Create(apiOp *types.APIRequest, schema *types.APISchema, params types.APIObject) (types.APIObject, error) {
	var (
		resp *unstructured.Unstructured
	)

	input := params.Data()

	if input == nil {
		input = data.Object{}
	}

	name := types.Name(input)
	ns := types.Namespace(input)
	if name == "" && input.String("metadata", "generateName") == "" {
		input.SetNested(schema.ID[0:1]+"-", "metadata", "generatedName")
	}
	if ns == "" && apiOp.Namespace != "" {
		ns = apiOp.Namespace
		input.SetNested(ns, "metadata", "namespace")
	}

	gvk := attributes.GVK(schema)
	input["apiVersion"], input["kind"] = gvk.ToAPIVersionAndKind()

	buffer := &WarningBuffer{}
	k8sClient, err := metricsStore.Wrap(s.clientGetter.TableClient(apiOp, schema, ns, buffer))
	if err != nil {
		return types.APIObject{}, err
	}

	opts := metav1.CreateOptions{}
	if err := decodeParams(apiOp, &opts); err != nil {
		return types.APIObject{}, err
	}

	resp, err = k8sClient.Create(apiOp, &unstructured.Unstructured{Object: input}, opts)
	rowToObject(resp)
	apiObject := toAPI(schema, resp, buffer.Warnings)
	return apiObject, err
}

// Update updates a single object in the store.
func (s *Store) Update(apiOp *types.APIRequest, schema *types.APISchema, params types.APIObject, id string) (types.APIObject, error) {
	var (
		err   error
		input = params.Data()
	)

	ns := types.Namespace(input)
	buffer := &WarningBuffer{}
	k8sClient, err := metricsStore.Wrap(s.clientGetter.TableClient(apiOp, schema, ns, buffer))
	if err != nil {
		return types.APIObject{}, err
	}

	if apiOp.Method == http.MethodPatch {
		bytes, err := ioutil.ReadAll(io.LimitReader(apiOp.Request.Body, 2<<20))
		if err != nil {
			return types.APIObject{}, err
		}

		pType := apitypes.StrategicMergePatchType
		if apiOp.Request.Header.Get("content-type") == string(apitypes.JSONPatchType) {
			pType = apitypes.JSONPatchType
		}

		opts := metav1.PatchOptions{}
		if err := decodeParams(apiOp, &opts); err != nil {
			return types.APIObject{}, err
		}

		if pType == apitypes.StrategicMergePatchType {
			data := map[string]interface{}{}
			if err := json.Unmarshal(bytes, &data); err != nil {
				return types.APIObject{}, err
			}
			data = moveFromUnderscore(data)
			bytes, err = json.Marshal(data)
			if err != nil {
				return types.APIObject{}, err
			}
		}

		resp, err := k8sClient.Patch(apiOp, id, pType, bytes, opts)
		if err != nil {
			return types.APIObject{}, err
		}

		return toAPI(schema, resp, buffer.Warnings), nil
	}

	resourceVersion := input.String("metadata", "resourceVersion")
	if resourceVersion == "" {
		return types.APIObject{}, fmt.Errorf("metadata.resourceVersion is required for update")
	}

	opts := metav1.UpdateOptions{}
	if err := decodeParams(apiOp, &opts); err != nil {
		return types.APIObject{}, err
	}

	resp, err := k8sClient.Update(apiOp, &unstructured.Unstructured{Object: moveFromUnderscore(input)}, metav1.UpdateOptions{})
	if err != nil {
		return types.APIObject{}, err
	}

	rowToObject(resp)
	return toAPI(schema, resp, buffer.Warnings), nil
}

// Delete deletes an object from a store.
func (s *Store) Delete(apiOp *types.APIRequest, schema *types.APISchema, id string) (types.APIObject, error) {
	opts := metav1.DeleteOptions{}
	if err := decodeParams(apiOp, &opts); err != nil {
		return types.APIObject{}, nil
	}

	buffer := &WarningBuffer{}
	k8sClient, err := metricsStore.Wrap(s.clientGetter.TableClient(apiOp, schema, apiOp.Namespace, buffer))
	if err != nil {
		return types.APIObject{}, err
	}

	if err := k8sClient.Delete(apiOp, id, opts); err != nil {
		return types.APIObject{}, err
	}

	obj, warnings, err := s.byID(apiOp, schema, apiOp.Namespace, id)
	if err != nil {
		// ignore lookup error
		return types.APIObject{}, validation.ErrorCode{
			Status: http.StatusNoContent,
		}
	}
	return toAPI(schema, obj, warnings), nil
}

package huawei

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"

	"github.com/virtual-kubelet/virtual-kubelet/manager"
	"github.com/virtual-kubelet/virtual-kubelet/providers/huawei/auth"
	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	defaultApiEndpoint        = "https://cciback.cn-north-1.huaweicloud.com"
	podAnnotationNamespaceKey = "virtual-kubelet-namespace"
)

// CCIProvider implements the virtual-kubelet provider interface and communicates with Huawei's CCI APIs.
type CCIProvider struct {
	appKey             string
	appSecret          string
	apiEndpoint        string
	region             string
	service            string
	project            string
	internalIP         string
	daemonEndpointPort int32
	nodeName           string
	operatingSystem    string
	client             *Client
	resourceManager    *manager.ResourceManager
	cpu                string
	memory             string
	pods               string
}

// Client represents the client config for Huawei.
type Client struct {
	Signer     *auth.Signer
	HTTPClient *http.Client
}

// NewCCIProvider creates a new CCI provider.
func NewCCIProvider(config string, rm *manager.ResourceManager, nodeName, operatingSystem string, internalIP string, daemonEndpointPort int32) (*CCIProvider, error) {
	p := CCIProvider{}

	if config != "" {
		f, err := os.Open(config)
		if err != nil {
			return nil, err
		}
		defer f.Close()

		if err := p.loadConfig(f); err != nil {
			return nil, err
		}
	}
	if appKey := os.Getenv("CCI_APP_KEP"); appKey != "" {
		p.appKey = appKey
	}
	if p.appKey == "" {
		return nil, errors.New("AppKey can not be empty please set CCI_APP_KEP")
	}
	if appSecret := os.Getenv("CCI_APP_SECRET"); appSecret != "" {
		p.appSecret = appSecret
	}
	if p.appSecret == "" {
		return nil, errors.New("AppSecret can not be empty please set CCI_APP_SECRET")
	}
	p.client.Signer = &auth.Signer{
		AppKey:    p.appKey,
		AppSecret: p.appSecret,
		Region:    p.region,
		Service:   p.service,
	}
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	p.client.HTTPClient = &http.Client{
		Transport: tr,
	}
	p.resourceManager = rm
	p.apiEndpoint = defaultApiEndpoint
	p.nodeName = nodeName
	p.operatingSystem = operatingSystem
	p.internalIP = internalIP
	p.daemonEndpointPort = daemonEndpointPort

	if err := p.createProject(); err != nil {
		return nil, err
	}
	return &p, nil
}

func (p *CCIProvider) createProject() error {
	// Create the createProject request url
	uri := p.apiEndpoint + "/api/v1/namespaces"
	// build the request
	project := &v1.Namespace{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Namespace",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: p.project,
		},
	}
	var bodyReader io.Reader
	body, err := json.Marshal(project)
	if err != nil {
		return err
	}
	if body != nil {
		bodyReader = bytes.NewReader(body)
	}

	r, err := http.NewRequest("POST", uri, bodyReader)
	if err != nil {
		return err
	}
	if err = p.signRequest(r); err != nil {
		return fmt.Errorf("Sign the request failed: %v", err)
	}
	_, err = p.client.HTTPClient.Do(r)
	return err
}

func (p *CCIProvider) signRequest(r *http.Request) error {
	r.Header.Add("content-type", "application/json; charset=utf-8")
	if err := p.client.Signer.Sign(r); err != nil {
		return fmt.Errorf("Sign the request failed: %v", err)
	}
	return nil
}

func (p *CCIProvider) setPodAnnotations(pod *v1.Pod) {
	pod.Namespace = p.project
	metav1.SetMetaDataAnnotation(&pod.ObjectMeta, podAnnotationNamespaceKey, pod.Namespace)
}

func (p *CCIProvider) deletePodAnnotations(pod *v1.Pod) {
	pod.Namespace = pod.Annotations[podAnnotationNamespaceKey]
	delete(pod.Annotations, podAnnotationNamespaceKey)
}

// CreatePod takes a Kubernetes Pod and deploys it within the huawei CCI provider.
func (p *CCIProvider) CreatePod(pod *v1.Pod) error {
	// Create the createPod request url
	p.setPodAnnotations(pod)
	uri := p.apiEndpoint + "/api/v1/namespaces/" + p.project + "/pods"
	// build the request
	var bodyReader io.Reader
	body, err := json.Marshal(pod)
	if err != nil {
		return err
	}
	if body != nil {
		bodyReader = bytes.NewReader(body)
	}

	r, err := http.NewRequest("POST", uri, bodyReader)
	if err != nil {
		return err
	}

	if err = p.signRequest(r); err != nil {
		return fmt.Errorf("Sign the request failed: %v", err)
	}
	_, err = p.client.HTTPClient.Do(r)
	return err
}

// UpdatePod takes a Kubernetes Pod and updates it within the huawei CCI provider.
func (p *CCIProvider) UpdatePod(pod *v1.Pod) error {
	// Create the updatePod request url
	p.setPodAnnotations(pod)
	uri := p.apiEndpoint + "/api/v1/namespaces/" + p.project + "/pods"
	// build the request
	var bodyReader io.Reader
	body, err := json.Marshal(pod)
	if err != nil {
		return err
	}
	if body != nil {
		bodyReader = bytes.NewReader(body)
	}

	r, err := http.NewRequest("PUT", uri, bodyReader)
	if err != nil {
		return err
	}

	if err = p.signRequest(r); err != nil {
		return fmt.Errorf("Sign the request failed: %v", err)
	}
	_, err = p.client.HTTPClient.Do(r)
	return err
}

// DeletePod takes a Kubernetes Pod and deletes it from the huawei CCI provider.
func (p *CCIProvider) DeletePod(pod *v1.Pod) error {
	// Create the deletePod request url
	uri := p.apiEndpoint + "/api/v1/namespaces/" + p.project + "/pods/" + pod.Name
	// build the request
	r, err := http.NewRequest("DELETE", uri, nil)
	if err != nil {
		return err
	}

	if err = p.signRequest(r); err != nil {
		return fmt.Errorf("Sign the request failed: %v", err)
	}
	_, err = p.client.HTTPClient.Do(r)
	return err
}

// GetPod retrieves a pod by name from the huawei CCI provider.
func (p *CCIProvider) GetPod(namespace, name string) (*v1.Pod, error) {
	// Create the getPod request url
	uri := p.apiEndpoint + "/api/v1/namespaces/" + p.project + "/pods/" + name
	r, err := http.NewRequest("GET", uri, nil)
	if err != nil {
		return nil, fmt.Errorf("Create get POD request failed: %v", err)
	}

	if err = p.signRequest(r); err != nil {
		return nil, fmt.Errorf("Sign the request failed: %v", err)
	}

	resp, err := p.client.HTTPClient.Do(r)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var pod v1.Pod
	if err = json.Unmarshal(body, &pod); err != nil {
		return nil, err
	}
	p.deletePodAnnotations(&pod)
	return &pod, nil
}

// GetContainerLogs retrieves the logs of a container by name from the huawei CCI provider.
func (p *CCIProvider) GetContainerLogs(namespace, podName, containerName string, tail int) (string, error) {
	return "", nil
}

// GetPodStatus retrieves the status of a pod by name from the huawei CCI provider.
func (p *CCIProvider) GetPodStatus(namespace, name string) (*v1.PodStatus, error) {
	pod, err := p.GetPod(namespace, name)
	if err != nil {
		return nil, err
	}

	if pod == nil {
		return nil, nil
	}

	return &pod.Status, nil
}

// GetPods retrieves a list of all pods running on the huawei CCI provider.
func (p *CCIProvider) GetPods() ([]*v1.Pod, error) {
	// Create the getPod request url
	uri := p.apiEndpoint + "/api/v1/namespaces/" + p.project + "/pods"
	r, err := http.NewRequest("GET", uri, nil)
	if err != nil {
		return nil, fmt.Errorf("Create get POD request failed: %v", err)
	}

	if err = p.signRequest(r); err != nil {
		return nil, fmt.Errorf("Sign the request failed: %v", err)
	}
	resp, err := p.client.HTTPClient.Do(r)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var pods []*v1.Pod
	if err = json.Unmarshal(body, &pods); err != nil {
		return nil, err
	}
	for _, pod := range pods {
		p.deletePodAnnotations(pod)
	}
	return pods, nil
}

// Capacity returns a resource list with the capacity constraints of the huawei CCI provider.
func (p *CCIProvider) Capacity() v1.ResourceList {
	return v1.ResourceList{
		"cpu":    resource.MustParse(p.cpu),
		"memory": resource.MustParse(p.memory),
		"pods":   resource.MustParse(p.pods),
	}
}

// NodeConditions returns a list of conditions (Ready, OutOfDisk, etc), which is
// polled periodically to update the node status within Kubernetes.
func (p *CCIProvider) NodeConditions() []v1.NodeCondition {
	// TODO: Make these dynamic and augment with custom CCI specific conditions of interest
	return []v1.NodeCondition{
		{
			Type:               "Ready",
			Status:             v1.ConditionTrue,
			LastHeartbeatTime:  metav1.Now(),
			LastTransitionTime: metav1.Now(),
			Reason:             "KubeletReady",
			Message:            "kubelet is ready.",
		},
		{
			Type:               "OutOfDisk",
			Status:             v1.ConditionFalse,
			LastHeartbeatTime:  metav1.Now(),
			LastTransitionTime: metav1.Now(),
			Reason:             "KubeletHasSufficientDisk",
			Message:            "kubelet has sufficient disk space available",
		},
		{
			Type:               "MemoryPressure",
			Status:             v1.ConditionFalse,
			LastHeartbeatTime:  metav1.Now(),
			LastTransitionTime: metav1.Now(),
			Reason:             "KubeletHasSufficientMemory",
			Message:            "kubelet has sufficient memory available",
		},
		{
			Type:               "DiskPressure",
			Status:             v1.ConditionFalse,
			LastHeartbeatTime:  metav1.Now(),
			LastTransitionTime: metav1.Now(),
			Reason:             "KubeletHasNoDiskPressure",
			Message:            "kubelet has no disk pressure",
		},
		{
			Type:               "NetworkUnavailable",
			Status:             v1.ConditionFalse,
			LastHeartbeatTime:  metav1.Now(),
			LastTransitionTime: metav1.Now(),
			Reason:             "RouteCreated",
			Message:            "RouteController created a route",
		},
	}
}

// NodeAddresses returns a list of addresses for the node status
// within Kubernetes.
func (p *CCIProvider) NodeAddresses() []v1.NodeAddress {
	// TODO: Make these dynamic and augment with custom CCI specific conditions of interest
	return []v1.NodeAddress{
		{
			Type:    "InternalIP",
			Address: p.internalIP,
		},
	}
}

// NodeDaemonEndpoints returns NodeDaemonEndpoints for the node status
// within Kubernetes.
func (p *CCIProvider) NodeDaemonEndpoints() *v1.NodeDaemonEndpoints {
	return &v1.NodeDaemonEndpoints{
		KubeletEndpoint: v1.DaemonEndpoint{
			Port: p.daemonEndpointPort,
		},
	}
}

// OperatingSystem returns the operating system the huawei CCI provider is for.
func (p *CCIProvider) OperatingSystem() string {
	return p.operatingSystem
}

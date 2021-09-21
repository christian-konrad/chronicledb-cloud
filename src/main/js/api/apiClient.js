class ApiClient {
    static async fetchSystemInfo() {
        const response = await fetch('/api/sys-info');
        return response.json();
    }

    static async fetchNodeInfo(nodeId) {
        const response = await fetch(`/api/sys-info/nodes/${nodeId}`);
        return response.json();
    }

    static async fetchReplicatedCounterIds() {
        const response = await fetch(`/api/counter/replicated/`);
        return response.json();
    }

    static async fetchReplicatedCounter(counterId) {
        const response = await fetch(`/api/counter/replicated/${counterId}`);
        return response.json();
    }

    static async createReplicatedCounter(counterId, { partitions = 3 } = {}) {
        const response = await fetch(`/api/counter/replicated/`, {
            method: 'POST',
            headers: {
                'Accept': 'application/json',
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({ id: counterId })
        });
        return response.json();
    }

    static async incrementReplicatedCounter(counterId) {
        const response = await fetch(`/api/counter/replicated/${counterId}/increment`, {
            method: 'POST'
        });
        return response.ok;
    }
}

export default ApiClient;
// TODO split into clients per app logic

class ApiClient {
    static async fetchSystemInfo() {
        const response = await fetch('/api/sys-info');
        return response.json();
    }

    static async fetchNodeInfo(nodeId) {
        const response = await fetch(`/api/sys-info/nodes/${nodeId}`);
        return response.json();
    }

    static async fetchMetaData() {
        const response = await fetch('/api/sys-info/metadata');
        return response.json();
    }

    static async fetchRaftGroupInfo(raftGroupId) {
        const response = await fetch(`/api/sys-info/raft-groups/${raftGroupId}`);
        return response.json();
    }

    static async fetchRaftGroupDivisions(raftGroupId) {
        const response = await fetch(`/api/sys-info/raft-groups/${raftGroupId}/divisions`);
        return response.json();
    }

    static async fetchClusterHealth() {
        const response = await fetch(`/api/sys-info/cluster/health`);
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
            body: JSON.stringify({ id: counterId, partitionsCount: partitions })
        });
        return response.json();
    }

    static async incrementReplicatedCounter(counterId) {
        const response = await fetch(`/api/counter/replicated/${counterId}/increment`, {
            method: 'POST'
        });
        return response.ok;
    }

    static async fetchEventStreamNames() {
        const response = await fetch(`/api/event-store/streams`);
        return response.json();
    }

    static async fetchEventStreamInfo(streamName) {
        const response = await fetch(`/api/event-store/streams/${streamName}/info`);
        return response.json();
    }

    static async pushEvents(streamName, events) {
        const response = await fetch(`/api/event-store/streams/${streamName}/events`, {
            method: 'POST',
            headers: {
                'Accept': 'application/json',
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({ events })
        });
        return response;
    }

    static async pushEvent(streamName, event) {
        return this.pushEvents(streamName, [event]);
    }


    static async getAggregate({ streamName, aggregateType, attributeName="ALL", range={} }={}) {
        // TODO validate params
        let path = `/api/event-store/streams/${streamName}/aggregates/${aggregateType}/${attributeName}`;
        if (range.lower != null && range.upper != null) {
            const queryParams = new URLSearchParams({
                rangeStart: range.lower,
                rangeEnd: range.upper
            });
            if (range.lowerInclusive != null) queryParams.append("lowerInclusive", range.lowerInclusive);
            if (range.upperInclusive != null) queryParams.append("lowerInclusive", range.upperInclusive);
            path = `${path}?${queryParams}`;
        }

        const response = await fetch(path);
        return response.text();
    }
}

export default ApiClient;
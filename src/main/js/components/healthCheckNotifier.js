import React from "react";
import ApiClient from "../api/apiClient";
import {SnackbarProvider, withSnackbar} from "notistack";

class HealthCheckNotifier extends React.Component {

    constructor(props) {
        super(props);
        this.onHealthInfoReceived = props.onHealthInfoReceived;
        this.state = { clusterHealth: null, unhealthySnackbarKey: null };
        this.fetchClusterHealth = this.fetchClusterHealth.bind(this);
        this.notifyOfHealthChanges = this.notifyOfHealthChanges.bind(this);
    }

    notifyOfHealthChanges(clusterHealth) {
        const previousHealth = this.state.clusterHealth;
        if (!previousHealth) return;

        previousHealth.nodeHealths.forEach(previousNodeHealth => {
            const nodeId = previousNodeHealth.id;
            const newNodeHealth = clusterHealth.nodeHealths.find(nodeHealth => nodeHealth.id === nodeId);
            if (previousNodeHealth.connectionState !== newNodeHealth.connectionState) {
                this.props.closeSnackbar(this.state[`snackbarKey_${nodeId}`]);
                const snackbarKey = this.props.enqueueSnackbar(`Node ${nodeId} is ${newNodeHealth.connectionState}`, {
                    variant: newNodeHealth.connectionState === 'DISCONNECTED' ? 'error'
                        : newNodeHealth.connectionState === 'INTERRUPTED' ? 'warning' : 'info',
                    persist: newNodeHealth.connectionState !== 'CONNECTED',
                });
                this.setState({ [`snackbarKey_${nodeId}`]: snackbarKey });
            }
        });

        if (previousHealth.healthy !== clusterHealth.healthy) {
            if (!clusterHealth.healthy) {
                const unhealthySnackbarKey = this.props.enqueueSnackbar('Too many nodes down; the cluster is unhealthy: Not able to receive requests', {
                    variant: 'error',
                    persist: true,
                });
                this.setState({ unhealthySnackbarKey });
            } else {
                this.props.closeSnackbar(this.state.unhealthySnackbarKey);
                this.props.enqueueSnackbar('Quorum is back, cluster health restored', {
                    variant: 'success',
                });
            }
        }
    }

    fetchClusterHealth() {
        ApiClient.fetchClusterHealth()
            .then(clusterHealth => {
                this.notifyOfHealthChanges(clusterHealth);
                if (typeof this.onHealthInfoReceived === 'function')
                    this.onHealthInfoReceived(clusterHealth);
                this.setState({ clusterHealth });
            });
        // TODO on timeout, also show unhealthy notification
    }

    componentDidMount() {
        this.fetchClusterHealth();
        this.fetchClusterHealthInterval = setInterval(() => {
            this.fetchClusterHealth();
        }, 1000);
    }

    componentWillUnmount() {
        clearInterval(this.fetchClusterHealthInterval);
    }

    render() {
        return <></>;
    }
}

export default withSnackbar(HealthCheckNotifier);

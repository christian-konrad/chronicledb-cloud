import React, { Component } from 'react';
import ApiClient from "../api/apiClient";
import Skeleton from '@material-ui/lab/Skeleton';
import ReactJson from 'react-json-view';
import {withStyles} from '@material-ui/core/styles';
import ErrorBoundary from "./error/errorBoundary";

const useStyles = theme => ({
    listItem: {
        borderBottom: '1px solid #f1f1f1',
    },
    listItemText: {
        display: 'flex',
    },
    muiTypography: {
        subtitle2: {
            fontWeight: 600,
        }
    },
    metaDataBox: {

    }
});

class MetaData extends Component {

    constructor(props) {
        super(props);
        this.state = { metaData: null };
    }

    componentDidMount() {
        ApiClient.fetchMetaData()
            .then(metaData => this.setState({ metaData }));
    }

    render() {
        if (!this.state.metaData) return <Skeleton />

        const { classes } = this.props;
        const metaData = this.state.metaData;

        return (
            <>
                <ErrorBoundary>
                    <div className={classes.metaDataBox}>
                        <ReactJson src={metaData} name={false} displayDataTypes={false} />
                    </div>
                </ErrorBoundary>
            </>
        )
    }
}
export default withStyles(useStyles)(MetaData)
